//! Networking layer: libp2p swarm with QUIC transport and Gossipsub for the CL.
//!
//! This module carries **consensus-layer** traffic only (beacon blocks, payload
//! envelopes, blob sidecars, PTC attestations). Execution-layer blob propagation
//! runs over a separate TCP transport — see `el_net.rs`.
//!
//! The network module is strictly decoupled from the state machine — it builds and
//! configures the swarm, and the state machine drives it via the event loop.

use crate::types::DATA_COLUMN_SIDECAR_SUBNET_COUNT;
use libp2p::{
    gossipsub::{self, IdentTopic, MessageAuthenticity},
    identity,
    swarm::NetworkBehaviour,
    Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use std::time::Duration;
use tracing::info;

// ---------------------------------------------------------------------------
// Gossipsub topic constants
// ---------------------------------------------------------------------------

/// CL topic: signed beacon blocks (proposed by the proposer, containing the builder's bid).
pub const TOPIC_CL_BEACON_BLOCK: &str = "/cl/beacon_block/1";
/// CL topic: signed execution payload envelopes.
pub const TOPIC_CL_PAYLOAD_ENVELOPE: &str = "/cl/payload_envelope/1";
/// CL topic: blob sidecars.
pub const TOPIC_CL_BLOB_SIDECAR: &str = "/cl/blob_sidecar/1";

/// CL topic prefix for data column sidecars (one gossipsub subnet per column
/// under Fulu): `/cl/data_column_sidecar/{subnet}/1`.
pub const TOPIC_CL_DATA_COLUMN_PREFIX: &str = "/cl/data_column_sidecar";

/// Gossipsub topic for a data column subnet.
pub fn data_column_topic(subnet: u64) -> IdentTopic {
    IdentTopic::new(format!("{TOPIC_CL_DATA_COLUMN_PREFIX}/{subnet}/1"))
}

/// Map a column index to its data column subnet.
pub fn subnet_for_column(index: u64) -> u64 {
    index % DATA_COLUMN_SIDECAR_SUBNET_COUNT
}

/// Parse the subnet id out of a data column topic string, if it is one.
pub fn subnet_from_topic(topic: &str) -> Option<u64> {
    let rest = topic.strip_prefix(TOPIC_CL_DATA_COLUMN_PREFIX)?;
    // rest is like "/{subnet}/1"
    let mut parts = rest.trim_start_matches('/').split('/');
    parts.next()?.parse().ok()
}

/// All data column subnets (0..count) — the set a block source / supernode joins.
pub fn all_column_subnets() -> Vec<u64> {
    (0..DATA_COLUMN_SIDECAR_SUBNET_COUNT).collect()
}

/// The always-on CL topics (baseline full-message path). The payload-envelope
/// topic is joined separately so zk-attesters can opt out of it (see
/// [`subscribe_all`]).
pub fn all_topics() -> Vec<IdentTopic> {
    vec![
        IdentTopic::new(TOPIC_CL_BEACON_BLOCK),
        IdentTopic::new(TOPIC_CL_BLOB_SIDECAR),
    ]
}

// ---------------------------------------------------------------------------
// Combined network behaviour
// ---------------------------------------------------------------------------

/// The composed libp2p behaviour for the simulator.
///
/// - `gossipsub`: CL gossip (bids, envelopes, sidecars, PTC attestations).
///
/// Execution-layer blob propagation does not use libp2p; it runs over TCP in
/// `el_net.rs`.
#[derive(NetworkBehaviour)]
pub struct SimBehaviour {
    pub gossipsub: gossipsub::Behaviour,
}

// ---------------------------------------------------------------------------
// Swarm construction
// ---------------------------------------------------------------------------

/// Build a deterministic libp2p keypair from a seed.
pub fn keypair_from_seed(seed: u64) -> identity::Keypair {
    // Stretch the u64 seed into 32 bytes for ed25519 secret key.
    let mut secret = [0u8; 32];
    let seed_bytes = seed.to_le_bytes();
    for (i, &b) in seed_bytes.iter().enumerate() {
        secret[i] = b;
        // Simple spread to fill remaining bytes deterministically
        secret[i + 8] = b.wrapping_mul(31);
        secret[i + 16] = b.wrapping_mul(47);
        secret[i + 24] = b.wrapping_mul(59);
    }
    identity::Keypair::ed25519_from_bytes(secret).expect("valid ed25519 key from seed")
}

/// Build and configure the libp2p swarm.
///
/// Returns the swarm and the local `PeerId`.
pub fn build_swarm(seed: u64, listen_port: u16) -> (Swarm<SimBehaviour>, PeerId) {
    let keypair = keypair_from_seed(seed);
    let local_peer_id = PeerId::from(keypair.public());
    info!(%local_peer_id, "building swarm");

    // -- Gossipsub --
    // Ethereum Mainnet Gossipsub parameters:
    // D=8, D_low=6, D_high=12, D_lazy=6
    // Heartbeat=700ms, FanoutTTL=60s, HistoryLength=5, HistoryGossip=3
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_millis(700)) // Standard is 700ms
        .mesh_n_low(6)
        .mesh_n(8)
        .mesh_n_high(12)
        .gossip_lazy(6)
        .history_length(5)
        .history_gossip(3)
        // Full 128 KiB blob sidecars (and their JSON envelope) exceed gossipsub's
        // default 64 KiB transmit limit, which would silently drop them. Raise the
        // ceiling so both the baseline full-sidecar path and partial cell messages
        // actually propagate.
        .max_transmit_size(16 * 1024 * 1024)
        .validation_mode(gossipsub::ValidationMode::Permissive)
        .build()
        .expect("valid gossipsub config");

    let gossipsub = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(keypair.clone()),
        gossipsub_config,
    )
    .expect("valid gossipsub behaviour");

    let behaviour = SimBehaviour { gossipsub };

    let swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_quic()
        .with_dns()
        .expect("valid DNS configuration")
        .with_behaviour(|_key| behaviour)
        .expect("valid behaviour")
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();

    // Listen on QUIC
    let listen_addr: Multiaddr = format!("/ip4/0.0.0.0/udp/{listen_port}/quic-v1")
        .parse()
        .expect("valid multiaddr");

    let mut swarm = swarm;
    swarm.listen_on(listen_addr).expect("listen on QUIC");

    (swarm, local_peer_id)
}

/// Subscribe the swarm to all simulation gossipsub topics.
///
/// When `enable_partial_columns` is set, the given `column_subnets` are joined
/// via [`subscribe_partial`] (enabling the gossipsub 1.3 partial-message
/// protocol) so blobs propagate as cell-level deltas. Otherwise only the
/// baseline full-message topics are joined.
///
/// A CL client subscribes only to the subnets of the columns it custodies; the
/// block source (builder/proposer) subscribes to all subnets so it can seed every
/// column, and a supernode does too. `column_subnets` carries that
/// per-node set.
///
/// `subscribe_payload_envelope` gates joining the payload-envelope topic: a
/// zk-attester (EIP-8142) passes `false` and instead receives only the
/// payload-blob cells for its custody columns (partial payload).
pub fn subscribe_all(
    swarm: &mut Swarm<SimBehaviour>,
    enable_partial_columns: bool,
    subscribe_payload_envelope: bool,
    column_subnets: &[u64],
) {
    let mut topics = all_topics();
    if subscribe_payload_envelope {
        topics.push(IdentTopic::new(TOPIC_CL_PAYLOAD_ENVELOPE));
    }
    for topic in topics {
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic)
            .expect("subscribe to topic");
        info!(topic = %topic, "subscribed");
    }

    if enable_partial_columns {
        for &subnet in column_subnets {
            subscribe_partial(swarm, &data_column_topic(subnet));
        }
        info!(
            subnets = column_subnets.len(),
            "subscribed to custody data column subnets with partial messages enabled"
        );
    }
}

/// Subscribe to a topic with the gossipsub 1.3 partial-message protocol enabled.
///
/// `enable_partials_for_topic` must be called *before* `subscribe` — the
/// behaviour ignores it once the topic is already in the mesh.
pub fn subscribe_partial(swarm: &mut Swarm<SimBehaviour>, topic: &IdentTopic) {
    let gossipsub = &mut swarm.behaviour_mut().gossipsub;
    gossipsub.enable_partials_for_topic(topic.hash(), true);
    gossipsub.subscribe(topic).expect("subscribe partial topic");
}

/// Dial a list of bootstrap peer multiaddrs.
pub fn dial_peers(swarm: &mut Swarm<SimBehaviour>, peers: &[Multiaddr]) {
    for addr in peers {
        info!(peer = %addr, "dialing peer");
        swarm.dial(addr.clone()).expect("dial peer");
    }
}
