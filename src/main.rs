//! Ethereum blob propagation simulator — Shadow-compatible entry point.
//!
//! Parses CLI arguments to assign the node's persona and starts the
//! networking stack + slot ticker state machine.

#[macro_use]
mod events;
mod el_net;
mod metrics;
mod network;
mod partial;
mod state_machine;
mod types;

use crate::el_net::spawn_el_network;
use crate::metrics::BandwidthMetrics;
use crate::network::{
    all_column_subnets, build_swarm, dial_peers, subnet_for_column, subscribe_all,
};
use crate::state_machine::{run_blob_spammer, run_node};
use crate::types::{
    custody_columns_for_seed, payload_blob_count, NodeRoles, Role, BLOBS_PER_SLOT,
    CUSTODY_SUBSET_SIZE, EXEC_PAYLOAD_SIZE, MAX_BLOBS_PER_BLOCK, USABLE_BYTES_PER_BLOB,
};

use clap::Parser;
use libp2p::Multiaddr;
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Shadow-compatible Ethereum blob propagation simulator.
///
/// Simulates EIP-7732 (ePBS) and EIP-8070 (Sparse Blobpool) message flows
/// using real libp2p (QUIC transport) with mocked cryptographic payloads.
#[derive(Parser, Debug)]
#[command(name = "blob-sim", version, about)]
struct Cli {
    /// Node role (repeatable): proposer, builder, validator, blob-spammer.
    /// Non-builder CL nodes choose sampler/provider fetch behavior per blob.
    #[arg(long = "role", required = true)]
    roles: Vec<Role>,

    /// CL QUIC listen port (0 = OS-assigned)
    #[arg(long, default_value_t = 0)]
    port: u16,

    /// EL TCP listen port (0 = OS-assigned)
    #[arg(long = "el-port", default_value_t = 0)]
    el_port: u16,

    /// Deterministic RNG seed
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// CL bootstrap peer multiaddrs to dial (QUIC)
    #[arg(long = "peer")]
    peers: Vec<Multiaddr>,

    /// EL peer socket addresses to dial over TCP (e.g. 127.0.0.1:9101)
    #[arg(long = "el-peer")]
    el_peers: Vec<SocketAddr>,

    /// Number of 12-second slots to simulate
    #[arg(long, default_value_t = 10)]
    slots: u64,

    /// Blobs produced per slot by a blob-spammer node (the spam rate knob),
    /// paced evenly across the slot.
    #[arg(long = "blobs-per-slot", default_value_t = BLOBS_PER_SLOT)]
    blobs_per_slot: usize,

    /// Size in bytes of the execution-payload body a builder reveals each slot in
    /// its `SignedExecutionPayloadEnvelope` (the payload-reveal over CL). Only
    /// builders publish envelopes, so this is inert for other roles.
    #[arg(long = "exec-payload-size", default_value_t = EXEC_PAYLOAD_SIZE)]
    exec_payload_size: usize,

    /// Per-node id mixed into the RNG seed so that blob-spammers launched with the
    /// same --seed still produce distinct blobs. The launcher assigns a unique value.
    #[arg(long = "node-id", default_value_t = 0)]
    node_id: u64,

    /// Enable gossipsub 1.3 partial messages + cell-level deltas for CL blob
    /// propagation (data column sidecars). When set, nodes subscribe to the data
    /// column subnets with the partial protocol and derive custody columns from
    /// blobs already held by the local EL (`engine_getBlobs` analog — a local
    /// read, not a network fetch); when unset, only the baseline full
    /// blob-sidecar path is used.
    #[arg(long = "enable-partial-columns", default_value_t = false)]
    enable_partial_columns: bool,

    /// Disable the local `engine_getBlobs` analog (mirrors Lighthouse's
    /// `--disable-get-blobs`). With `--enable-partial-columns`, a node then
    /// ignores its local EL blob pool and pulls all of its custody columns'
    /// cells from peers over CL as cell-level deltas. No effect unless partial
    /// columns are enabled.
    #[arg(long = "disable-get-blobs", default_value_t = false)]
    disable_get_blobs: bool,

    /// Enable blocks-in-blobs (EIP-8142): the builder additionally encodes the
    /// execution payload into payload-blobs (commitments first, sharing the
    /// per-block blob budget) and seeds them onto the data column subnets, so the
    /// payload propagates over both the envelope topic and columns. Implies
    /// `--enable-partial-columns`. A node with the `zk-attester` role skips the
    /// payload-envelope topic and relies on the column path.
    #[arg(long = "blocks-in-blobs", default_value_t = false)]
    blocks_in_blobs: bool,

    /// Number of stable custody columns each non-builder CL node subscribes to and
    /// fetches cells for (out of `NUM_CUSTODY_COLUMNS` = 128). Builders/proposers
    /// always custody all columns. Clamped to 128.
    #[arg(long = "custody-columns", default_value_t = CUSTODY_SUBSET_SIZE)]
    custody_columns: usize,

    /// Maximum blobs a builder includes in one block. Under blocks-in-blobs the
    /// payload-blobs share this budget with EL blobs (payload-blobs come first), so
    /// it must cover `ceil(exec_payload_size / 126976)`.
    #[arg(long = "max-blobs-per-block", default_value_t = MAX_BLOBS_PER_BLOCK)]
    max_blobs_per_block: usize,
}

// A single-threaded (current-thread) runtime: under Shadow every guest thread is
// simulated on a deterministic scheduler, so tokio's multi-threaded work-stealing
// runtime only adds simulated context-switch overhead and scheduling noise without
// any parallelism benefit. One cooperatively-polled thread keeps runs lean and
// closer to deterministic.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Initialize tracing (controlled by RUST_LOG env var, default = info).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let roles = NodeRoles::from_roles(&cli.roles);

    // Payload-blobs share the per-block blob budget with EL blobs (EIP-8142), so an
    // execution payload that needs more than the budget's payload-blobs cannot
    // fit — reject it rather than silently publishing an over-budget block.
    if cli.blocks_in_blobs {
        let n_payload = payload_blob_count(cli.exec_payload_size);
        if n_payload > cli.max_blobs_per_block {
            eprintln!(
                "error: --blocks-in-blobs execution payload of {} bytes needs {} payload-blobs, \
                 exceeding --max-blobs-per-block ({}). Reduce --exec-payload-size to at most {} \
                 bytes or raise --max-blobs-per-block.",
                cli.exec_payload_size,
                n_payload,
                cli.max_blobs_per_block,
                cli.max_blobs_per_block * USABLE_BYTES_PER_BLOB,
            );
            std::process::exit(1);
        }
    }

    info!(
        roles = %roles,
        port = cli.port,
        el_port = cli.el_port,
        seed = cli.seed,
        peers = ?cli.peers,
        el_peers = ?cli.el_peers,
        slots = cli.slots,
        "blob-sim starting"
    );

    // A blob-spammer is EL-only: it never builds or joins the CL swarm. Run its
    // dedicated loop over just the EL/TCP layer and exit.
    if roles.is_blob_spammer() {
        let mut el = spawn_el_network(cli.el_port, cli.el_peers);
        let mut metrics = BandwidthMetrics::new(&roles);
        run_blob_spammer(
            &roles,
            &mut el,
            cli.seed,
            cli.node_id,
            cli.slots,
            cli.blobs_per_slot,
            &mut metrics,
        )
        .await;
        info!("blob-spammer finished, shutting down");
        return;
    }

    // Build the CL libp2p swarm (QUIC)
    let (mut swarm, local_peer_id) = build_swarm(cli.seed, cli.port);
    info!(%local_peer_id, "swarm built");

    // Blocks-in-blobs propagates the payload over the data column subnets, which
    // in this sim is the partial-message path — so the mode implies it.
    let enable_partial = cli.enable_partial_columns || cli.blocks_in_blobs;
    // A zk-attester (EIP-8142) does not subscribe to the payload-envelope topic;
    // it only receives the payload-blob cells for its custody columns (partial
    // payload — a non-supernode does not reconstruct the full payload).
    let subscribe_envelope = !roles.is_zk_attester();

    // A CL client subscribes only to the subnets of the columns it custodies; the
    // block source (builder/proposer) must seed every column, so it joins all
    // subnets (as a supernode — deferred — would). Column index == subnet here.
    let column_subnets: Vec<u64> = if roles.is_builder() || roles.is_proposer() {
        all_column_subnets()
    } else {
        custody_columns_for_seed(cli.seed, cli.custody_columns)
            .into_iter()
            .map(subnet_for_column)
            .collect()
    };
    info!(
        zk_attester = roles.is_zk_attester(),
        blocks_in_blobs = cli.blocks_in_blobs,
        subscribe_envelope,
        custody_subnets = column_subnets.len(),
        "CL subscription profile"
    );

    // Subscribe to gossipsub topics (data column subnets use partial messages when
    // partial columns are enabled; zk-attesters skip the payload-envelope topic).
    subscribe_all(
        &mut swarm,
        enable_partial,
        subscribe_envelope,
        &column_subnets,
    );

    // Dial bootstrap peers
    if !cli.peers.is_empty() {
        dial_peers(&mut swarm, &cli.peers);
    }

    // Spawn the EL networking actor (TCP) and connect to EL peers.
    let mut el = spawn_el_network(cli.el_port, cli.el_peers);

    // Create bandwidth metrics tracker
    let mut metrics = BandwidthMetrics::new(&roles);

    // Run the state machine
    run_node(
        &roles,
        &mut swarm,
        &mut el,
        cli.seed,
        cli.slots,
        &mut metrics,
        enable_partial,
        cli.disable_get_blobs,
        cli.exec_payload_size,
        cli.blocks_in_blobs,
        cli.custody_columns,
        cli.max_blobs_per_block,
    )
    .await;
}
