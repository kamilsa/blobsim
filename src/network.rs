//! Networking layer: libp2p swarm with QUIC transport, Gossipsub, and Request-Response.
//!
//! The network module is strictly decoupled from the state machine — it builds and
//! configures the swarm, and the state machine drives it via the event loop.

use libp2p::{
    gossipsub::{self, IdentTopic, MessageAuthenticity},
    identity,
    request_response::{self, ProtocolSupport},
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
/// CL topic: PTC attestation messages.
pub const TOPIC_CL_PTC_ATTESTATION: &str = "/cl/ptc_attestation/1";
/// EL topic: blob hash announcements (simulating devp2p eth/71).
pub const TOPIC_EL_BLOB_HASH: &str = "/el/blob_hash/1";

/// All topics a node should subscribe to.
pub fn all_topics() -> Vec<IdentTopic> {
    vec![
        IdentTopic::new(TOPIC_CL_BEACON_BLOCK),
        IdentTopic::new(TOPIC_CL_PAYLOAD_ENVELOPE),
        IdentTopic::new(TOPIC_CL_BLOB_SIDECAR),
        IdentTopic::new(TOPIC_CL_PTC_ATTESTATION),
        IdentTopic::new(TOPIC_EL_BLOB_HASH),
    ]
}

// ---------------------------------------------------------------------------
// Request-Response protocol name
// ---------------------------------------------------------------------------

/// Protocol name for the simulated devp2p overlay (custody-cell / full-payload).
pub const DEVP2P_PROTOCOL: &str = "/sim/devp2p/1";

// ---------------------------------------------------------------------------
// Combined network behaviour
// ---------------------------------------------------------------------------

/// The composed libp2p behaviour for the simulator.
///
/// - `gossipsub`: CL gossip (bids, envelopes, sidecars, PTC attestations) + EL blob hash gossip.
/// - `req_res`: EL devp2p overlay for custody-cell and full-payload request/response.
#[derive(NetworkBehaviour)]
pub struct SimBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub req_res: request_response::json::Behaviour<crate::types::SimRequest, crate::types::SimResponse>,
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
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(1))
        .validation_mode(gossipsub::ValidationMode::Permissive)
        .build()
        .expect("valid gossipsub config");

    let gossipsub = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(keypair.clone()),
        gossipsub_config,
    )
    .expect("valid gossipsub behaviour");

    // -- Request-Response (JSON codec) --
    let req_res = request_response::json::Behaviour::new(
        [(
            libp2p::StreamProtocol::new(DEVP2P_PROTOCOL),
            ProtocolSupport::Full,
        )],
        request_response::Config::default(),
    );

    let behaviour = SimBehaviour { gossipsub, req_res };

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
pub fn subscribe_all(swarm: &mut Swarm<SimBehaviour>) {
    for topic in all_topics() {
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic)
            .expect("subscribe to topic");
        info!(topic = %topic, "subscribed");
    }
}

/// Dial a list of bootstrap peer multiaddrs.
pub fn dial_peers(swarm: &mut Swarm<SimBehaviour>, peers: &[Multiaddr]) {
    for addr in peers {
        info!(peer = %addr, "dialing peer");
        swarm.dial(addr.clone()).expect("dial peer");
    }
}
