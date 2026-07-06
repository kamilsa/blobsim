//! Ethereum blob propagation simulator — Shadow-compatible entry point.
//!
//! Parses CLI arguments to assign the node's persona and starts the
//! networking stack + slot ticker state machine.

mod el_net;
mod metrics;
mod network;
mod partial;
mod state_machine;
mod types;

use crate::el_net::spawn_el_network;
use crate::metrics::BandwidthMetrics;
use crate::network::{build_swarm, dial_peers, subscribe_all};
use crate::state_machine::{run_blob_spammer, run_node};
use crate::types::{NodeRoles, Role, BLOBS_PER_SLOT};

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
}

#[tokio::main]
async fn main() {
    // Initialize tracing (controlled by RUST_LOG env var, default = info).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let roles = NodeRoles::from_roles(&cli.roles);

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

    // Subscribe to all gossipsub topics (data column subnets use partial
    // messages when --enable-partial-columns is set).
    subscribe_all(&mut swarm, cli.enable_partial_columns);

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
        cli.enable_partial_columns,
        cli.disable_get_blobs,
    )
    .await;
}
