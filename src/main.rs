//! Ethereum blob propagation simulator — Shadow-compatible entry point.
//!
//! Parses CLI arguments to assign the node's persona and starts the
//! networking stack + slot ticker state machine.

mod metrics;
mod network;
mod state_machine;
mod types;

use crate::metrics::BandwidthMetrics;
use crate::network::{build_swarm, dial_peers, subscribe_all};
use crate::state_machine::run_node;
use crate::types::{NodeRoles, Role};

use clap::Parser;
use libp2p::Multiaddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Shadow-compatible Ethereum blob propagation simulator.
///
/// Simulates EIP-7732 (ePBS) and EIP-8070 (Sparse Blobpool) message flows
/// using real libp2p (QUIC transport) with mocked cryptographic payloads.
#[derive(Parser, Debug)]
#[command(name = "blob-sim", version, about)]
struct Cli {
    /// Node role (repeatable): builder, sampler, provider, ptc.
    /// Sampler and provider are mutually exclusive.
    #[arg(long = "role", required = true)]
    roles: Vec<Role>,

    /// QUIC listen port (0 = OS-assigned)
    #[arg(long, default_value_t = 0)]
    port: u16,

    /// Deterministic RNG seed
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Bootstrap peer multiaddrs to dial
    #[arg(long = "peer")]
    peers: Vec<Multiaddr>,

    /// Number of 12-second slots to simulate
    #[arg(long, default_value_t = 10)]
    slots: u64,
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
        seed = cli.seed,
        peers = ?cli.peers,
        slots = cli.slots,
        "blob-sim starting"
    );

    // Build the libp2p swarm
    let (mut swarm, local_peer_id) = build_swarm(cli.seed, cli.port);
    info!(%local_peer_id, "swarm built");

    // Subscribe to all gossipsub topics
    subscribe_all(&mut swarm);

    // Dial bootstrap peers
    if !cli.peers.is_empty() {
        dial_peers(&mut swarm, &cli.peers);
    }

    // Create bandwidth metrics tracker
    let mut metrics = BandwidthMetrics::new(&roles);

    // Run the state machine
    run_node(&roles, &mut swarm, cli.seed, cli.slots, &mut metrics).await;
}
