//! State machine: 12-second slot ticker with role-based event logic.
//!
//! The state machine drives both networking layers and triggers broadcasts at the
//! correct phase within each 12-second slot:
//!   - CL gossip (beacon blocks, payload envelopes, blob sidecars, PTC attestations)
//!     over the libp2p/QUIC swarm (`network.rs`).
//!   - EL blob propagation (announce → request → serve) over the TCP layer
//!     (`el_net.rs`).

use crate::el_net::{ElEvent, ElHandle, ElPeerId};
use crate::metrics::BandwidthMetrics;
use crate::network::{
    SimBehaviour, TOPIC_CL_BEACON_BLOCK, TOPIC_CL_BLOB_SIDECAR, TOPIC_CL_PAYLOAD_ENVELOPE,
    TOPIC_CL_PTC_ATTESTATION,
};
use crate::types::*;

use alloy_rlp::Bytes;
use futures::StreamExt;
use libp2p::gossipsub::IdentTopic;
use libp2p::swarm::SwarmEvent;
use libp2p::Swarm;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Per-slot state tracked during event processing.
struct SlotState {
    /// Whether we received an execution payload envelope this slot.
    payload_received: bool,
}

impl SlotState {
    fn new() -> Self {
        Self {
            payload_received: false,
        }
    }
}

/// Run the node's main loop for `num_slots` slots.
pub async fn run_node(
    roles: &NodeRoles,
    swarm: &mut Swarm<SimBehaviour>,
    el: &mut ElHandle,
    seed: u64,
    num_slots: u64,
    metrics: &mut BandwidthMetrics,
) {
    let mut rng = StdRng::seed_from_u64(seed);
    let node_index = seed; // use seed as a simple unique index for this node

    // Number of connected EL/TCP peers, kept up to date from EL events. Used to
    // account fan-out bandwidth when the builder announces blob hashes.
    let mut el_peer_count: usize = 0;

    info!(%roles, num_slots, "starting slot ticker");

    for slot in 0..num_slots {
        info!(slot, %roles, "=== SLOT START ===");
        let slot_start = Instant::now();
        let mut slot_state = SlotState::new();

        // ---------------------------------------------------------------
        // t=0s — Proposal phase (proposer only)
        // ---------------------------------------------------------------
        if roles.is_proposer() {
            // Proposer creates a beacon block containing the builder's signed bid.
            // In a real network the proposer would select the winning bid from a
            // relay; here we use a dummy bid with builder_index = 0.
            let block = SignedBeaconBlock::dummy(slot, node_index, /*builder_index=*/ 0);
            publish_gossip(
                swarm,
                TOPIC_CL_BEACON_BLOCK,
                &GossipMessage::BeaconBlock(block),
                metrics,
            );
            info!(slot, "proposer: published beacon block (containing bid)");
        }

        // Drain events until t=4s
        drain_events_until(
            swarm,
            el,
            roles,
            &mut rng,
            slot_start + Duration::from_secs(4),
            metrics,
            &mut slot_state,
            &mut el_peer_count,
        )
        .await;

        // ---------------------------------------------------------------
        // t=4-6s — Payload & blob release phase (builder only)
        //
        // By this point the builder has seen the beacon block (containing
        // its bid) and knows it was selected. It publishes:
        //   1. Signed execution payload envelope on CL gossip
        //   2. Blob sidecars on CL gossip
        // ---------------------------------------------------------------
        if roles.is_builder() {
            // Publish signed execution payload envelope
            let envelope = SignedExecutionPayloadEnvelope::dummy(slot, node_index);
            publish_gossip(
                swarm,
                TOPIC_CL_PAYLOAD_ENVELOPE,
                &GossipMessage::Envelope(envelope),
                metrics,
            );
            info!(slot, "builder: published payload envelope");

            // Publish blob sidecars
            for i in 0..BLOBS_PER_SLOT as u64 {
                let sidecar = BlobSidecar::random(slot, i, &mut rng);
                publish_gossip(
                    swarm,
                    TOPIC_CL_BLOB_SIDECAR,
                    &GossipMessage::Sidecar(sidecar),
                    metrics,
                );
            }
            info!(
                slot,
                blobs = BLOBS_PER_SLOT,
                "builder: published blob sidecars"
            );
        }

        // Drain events until t=6s
        drain_events_until(
            swarm,
            el,
            roles,
            &mut rng,
            slot_start + Duration::from_secs(6),
            metrics,
            &mut slot_state,
            &mut el_peer_count,
        )
        .await;

        // Drain events until t=8s
        drain_events_until(
            swarm,
            el,
            roles,
            &mut rng,
            slot_start + Duration::from_secs(8),
            metrics,
            &mut slot_state,
            &mut el_peer_count,
        )
        .await;

        // ---------------------------------------------------------------
        // t=8s — PTC vote phase
        //
        // PTC members check whether they received the execution payload
        // envelope from the builder. If so, they vote Present; otherwise
        // Absent. The builder itself always considers the payload present.
        // ---------------------------------------------------------------
        if roles.is_ptc_member() {
            let payload_status = if slot_state.payload_received || roles.is_builder() {
                PayloadStatus::Present
            } else {
                PayloadStatus::Absent
            };
            let attestation = PayloadAttestationMessage {
                slot,
                validator_index: seed,
                payload_status,
                signature: vec![0xFF; 96],
            };
            publish_gossip(
                swarm,
                TOPIC_CL_PTC_ATTESTATION,
                &GossipMessage::PtcAttestation(attestation),
                metrics,
            );
            info!(slot, status = ?payload_status, "ptc: published payload attestation");
        }

        // Drain events until t=12s (slot boundary)
        drain_events_until(
            swarm,
            el,
            roles,
            &mut rng,
            slot_start + Duration::from_secs(12),
            metrics,
            &mut slot_state,
            &mut el_peer_count,
        )
        .await;

        // Emit per-slot bandwidth summary
        metrics.emit_slot_summary(slot);

        info!(slot, "=== SLOT END ===");
    }

    // Emit end-of-simulation summary
    metrics.emit_final_summary(num_slots);

    info!("all slots completed, shutting down");
}

// ---------------------------------------------------------------------------
// Blob-spammer (EL-only load generator)
// ---------------------------------------------------------------------------

/// Mix the base `--seed` with the node-unique `--node-id` so that blob-spammers
/// launched with the same seed still produce distinct blobs, while a given
/// `(seed, node_id)` pair stays reproducible across runs.
fn mix_seed(seed: u64, node_id: u64) -> u64 {
    seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(node_id)
}

/// Run an EL-only blob-spammer node.
///
/// Each 12s slot the spammer originates `blobs_per_slot` random blobs, announcing
/// their hashes over EL/TCP **paced evenly across the slot** (not all at once), and
/// serves any custody-cell / full-payload requests peers make in response. It never
/// touches the CL gossip layer.
pub async fn run_blob_spammer(
    roles: &NodeRoles,
    el: &mut ElHandle,
    seed: u64,
    node_id: u64,
    num_slots: u64,
    blobs_per_slot: usize,
    metrics: &mut BandwidthMetrics,
) {
    let mut rng = StdRng::seed_from_u64(mix_seed(seed, node_id));
    let mut el_peer_count: usize = 0;

    info!(%roles, num_slots, blobs_per_slot, node_id, "starting blob-spammer");

    for slot in 0..num_slots {
        info!(slot, %roles, "=== SLOT START ===");
        let slot_start = Instant::now();
        let slot_end = slot_start + Duration::from_secs(12);

        // Pace `blobs_per_slot` announcements evenly across the slot. The immediate
        // first tick means ticks land at 0, p, 2p, …, (N-1)p — all strictly before
        // the slot boundary — so exactly `blobs_per_slot` blobs are produced.
        let mut produced: usize = 0;
        let period = if blobs_per_slot > 0 {
            (Duration::from_secs(12) / blobs_per_slot as u32).max(Duration::from_nanos(1))
        } else {
            Duration::from_secs(12)
        };
        let mut ticker = tokio::time::interval(period);

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(slot_end) => {
                    break;
                }
                _ = ticker.tick(), if produced < blobs_per_slot => {
                    // Originate one random blob and announce its hash to EL peers.
                    let announce = BlobHashAnnounce::random(slot, 1, &mut rng);
                    let frame_bytes = ElMessage::Announce(announce.clone()).encode().len() + 4;
                    el.announce(announce);
                    // Account the fan-out: one frame per connected peer.
                    for _ in 0..el_peer_count {
                        metrics.record_el_announce_sent(frame_bytes);
                    }
                    produced += 1;
                    debug!(slot, produced, peers = el_peer_count, "spammer: announced blob");
                }
                ev = el.event_rx.recv() => {
                    if let Some(ev) = ev {
                        handle_el_event(el, roles, &mut rng, ev, metrics, &mut el_peer_count);
                    }
                }
            }
        }

        info!(slot, produced, peers = el_peer_count, "spammer: slot complete");
        metrics.emit_slot_summary(slot);
        info!(slot, "=== SLOT END ===");
    }

    metrics.emit_final_summary(num_slots);
    info!("blob-spammer: all slots completed");
}

// ---------------------------------------------------------------------------
// Event drain loop
// ---------------------------------------------------------------------------

/// Process events from both networking layers until the given deadline.
///
/// Uses `tokio::select!` to multiplex between the deadline timer, incoming CL
/// swarm events, and incoming EL/TCP events.
#[allow(clippy::too_many_arguments)]
async fn drain_events_until(
    swarm: &mut Swarm<SimBehaviour>,
    el: &mut ElHandle,
    roles: &NodeRoles,
    rng: &mut StdRng,
    deadline: Instant,
    metrics: &mut BandwidthMetrics,
    slot_state: &mut SlotState,
    el_peer_count: &mut usize,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                break;
            }
            event = swarm.select_next_some() => {
                handle_swarm_event(swarm, event, metrics, slot_state);
            }
            ev = el.event_rx.recv() => {
                if let Some(ev) = ev {
                    handle_el_event(el, roles, rng, ev, metrics, el_peer_count);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CL swarm event handling
// ---------------------------------------------------------------------------

/// Handle a single CL swarm event.
fn handle_swarm_event(
    swarm: &Swarm<SimBehaviour>,
    event: SwarmEvent<SimBehaviourEvent>,
    metrics: &mut BandwidthMetrics,
    slot_state: &mut SlotState,
) {
    match event {
        // -- Gossipsub message received --
        SwarmEvent::Behaviour(SimBehaviourEvent::Gossipsub(gossipsub::Event::Message {
            propagation_source,
            message_id,
            message,
        })) => {
            let topic = message.topic.to_string();
            let msg_bytes = message.data.len();
            debug!(%propagation_source, %message_id, %topic, msg_bytes, "gossip message received");

            // Record incoming bandwidth
            metrics.record_gossip_received(&topic, msg_bytes);

            // Gossipsub will automatically forward this message to all other
            // mesh peers on the topic (excluding the propagation source). Count
            // them so we can log and account for the outgoing forwarding bandwidth.
            let forward_peers = swarm
                .behaviour()
                .gossipsub
                .mesh_peers(&message.topic)
                .filter(|p| *p != &propagation_source)
                .count();
            if forward_peers > 0 {
                let forwarded_bytes = forward_peers * msg_bytes;
                debug!(%topic, forward_peers, msg_bytes, forwarded_bytes, "gossip message forwarded");
                metrics.record_gossip_forwarded(&topic, forwarded_bytes);
            }

            // Deserialize the wrapper
            if let Ok(msg) = serde_json::from_slice::<GossipMessage>(&message.data) {
                handle_gossip_message(msg, slot_state);
            } else {
                warn!(%topic, "failed to deserialize gossip message");
            }
        }

        // -- Connection established --
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            info!(%peer_id, "connection established");
        }

        // -- Connection closed --
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            info!(%peer_id, ?cause, "connection closed");
        }

        // -- New listen address --
        SwarmEvent::NewListenAddr { address, .. } => {
            info!(%address, "listening on");
        }

        _ => {}
    }
}

/// Handle a decoded CL gossip message (logging + per-slot state tracking).
fn handle_gossip_message(msg: GossipMessage, slot_state: &mut SlotState) {
    match msg {
        GossipMessage::BeaconBlock(block) => {
            info!(
                slot = block.slot,
                proposer = block.proposer_index,
                builder = block.signed_execution_payload_bid.message.builder_index,
                bid_gwei = block.signed_execution_payload_bid.message.bid_value_gwei,
                "received beacon block"
            );
        }

        GossipMessage::Envelope(env) => {
            info!(
                slot = env.slot,
                builder = env.builder_index,
                commitments = env.blob_kzg_commitments.len(),
                "received payload envelope"
            );
            slot_state.payload_received = true;
        }

        GossipMessage::Sidecar(sidecar) => {
            info!(
                slot = sidecar.slot,
                blob_index = sidecar.blob_index,
                "received blob sidecar"
            );
        }

        GossipMessage::PtcAttestation(att) => {
            info!(
                slot = att.slot,
                validator = att.validator_index,
                status = ?att.payload_status,
                "received PTC attestation"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// EL/TCP event handling
// ---------------------------------------------------------------------------

/// Handle a single EL/TCP event.
fn handle_el_event(
    el: &ElHandle,
    roles: &NodeRoles,
    rng: &mut StdRng,
    event: ElEvent,
    metrics: &mut BandwidthMetrics,
    el_peer_count: &mut usize,
) {
    match event {
        ElEvent::PeerConnected(peer) => {
            *el_peer_count += 1;
            info!(peer, peers = *el_peer_count, "EL: peer connected");
        }
        ElEvent::PeerDisconnected(peer) => {
            *el_peer_count = el_peer_count.saturating_sub(1);
            info!(peer, peers = *el_peer_count, "EL: peer disconnected");
        }
        ElEvent::Message { from, msg, bytes } => {
            handle_el_message(el, roles, rng, from, msg, bytes, metrics);
        }
    }
}

/// Dispatch an inbound EL message based on type and this node's roles.
fn handle_el_message(
    el: &ElHandle,
    roles: &NodeRoles,
    rng: &mut StdRng,
    from: ElPeerId,
    msg: ElMessage,
    bytes: usize,
    metrics: &mut BandwidthMetrics,
) {
    match msg {
        // -- Blob hash announcement: samplers/providers pull what they need --
        ElMessage::Announce(announce) => {
            metrics.record_el_announce_received(bytes);
            info!(
                slot = announce.slot,
                hashes = announce.blob_hashes.len(),
                from,
                "EL: received blob hash announce"
            );
            if roles.is_sampler() {
                send_custody_requests(el, rng, from, &announce, metrics);
            }
            if roles.is_provider() {
                send_full_payload_requests(el, from, &announce, metrics);
            }
        }

        // -- Incoming requests: the holder (builder) serves a response --
        ElMessage::CustodyRequest(req) => {
            metrics.record_request_received(bytes);
            info!(slot = req.slot, columns = ?req.column_indices, from, "EL: handling custody cell request");
            let cells: Vec<CustodyCell> = req
                .column_indices
                .iter()
                .map(|&column| CustodyCell {
                    column,
                    data: Bytes::from(random_bytes(rng, BYTES_PER_CELL)), // 2 KiB per cell
                })
                .collect();
            let response = ElMessage::CustodyResponse(CustodyCellResponse {
                slot: req.slot,
                blob_hash: req.blob_hash,
                cells,
            });
            let resp_bytes = response.encode().len() + 4;
            metrics.record_response_sent(resp_bytes);
            el.send(from, response);
        }

        ElMessage::FullPayloadRequest(req) => {
            metrics.record_request_received(bytes);
            info!(slot = req.slot, from, "EL: handling full payload request");
            let response = ElMessage::FullPayloadResponse(FullPayloadResponse {
                slot: req.slot,
                blob_hash: req.blob_hash,
                payload_data: Bytes::from(random_bytes(rng, BLOB_SIZE)),
            });
            let resp_bytes = response.encode().len() + 4;
            metrics.record_response_sent(resp_bytes);
            el.send(from, response);
        }

        // -- Incoming responses (sampler/provider side) --
        ElMessage::CustodyResponse(resp) => {
            metrics.record_response_received(bytes);
            info!(
                slot = resp.slot,
                cells = resp.cells.len(),
                from,
                "EL: received custody cells"
            );
        }
        ElMessage::FullPayloadResponse(resp) => {
            metrics.record_response_received(bytes);
            info!(
                slot = resp.slot,
                payload_size = resp.payload_data.len(),
                from,
                "EL: received full payload"
            );
        }
    }
}

/// Sampler: request a deterministic subset of custody columns (+1 random extra)
/// from the announcing peer, for each announced blob hash.
fn send_custody_requests(
    el: &ElHandle,
    rng: &mut StdRng,
    peer: ElPeerId,
    announce: &BlobHashAnnounce,
    metrics: &mut BandwidthMetrics,
) {
    // Deterministically choose custody columns + 1 random extra.
    let mut columns: HashSet<u64> = HashSet::new();
    while columns.len() < CUSTODY_SUBSET_SIZE {
        columns.insert(rng.gen_range(0..NUM_CUSTODY_COLUMNS));
    }
    columns.insert(rng.gen_range(0..NUM_CUSTODY_COLUMNS));
    let column_indices: Vec<u64> = columns.into_iter().collect();

    for blob_hash in &announce.blob_hashes {
        let request = ElMessage::CustodyRequest(CustodyCellRequest {
            slot: announce.slot,
            blob_hash: blob_hash.clone(),
            column_indices: column_indices.clone(),
        });
        let req_bytes = request.encode().len() + 4;
        info!(slot = announce.slot, columns = ?column_indices, peer, req_bytes, "sampler: sending custody cell request");
        metrics.record_request_sent(req_bytes);
        el.send(peer, request);
    }
}

/// Provider: request the full payload from the announcing peer, per blob hash.
fn send_full_payload_requests(
    el: &ElHandle,
    peer: ElPeerId,
    announce: &BlobHashAnnounce,
    metrics: &mut BandwidthMetrics,
) {
    for blob_hash in &announce.blob_hashes {
        let request = ElMessage::FullPayloadRequest(FullPayloadRequest {
            slot: announce.slot,
            blob_hash: blob_hash.clone(),
        });
        let req_bytes = request.encode().len() + 4;
        info!(
            slot = announce.slot,
            peer, req_bytes, "provider: sending full payload request"
        );
        metrics.record_request_sent(req_bytes);
        el.send(peer, request);
    }
}

// ---------------------------------------------------------------------------
// Gossip publish helper
// ---------------------------------------------------------------------------

/// Serialize a `GossipMessage` to JSON and publish it on the given topic.
fn publish_gossip(
    swarm: &mut Swarm<SimBehaviour>,
    topic_str: &str,
    msg: &GossipMessage,
    metrics: &mut BandwidthMetrics,
) {
    let topic = IdentTopic::new(topic_str);
    let data = serde_json::to_vec(msg).expect("serialize gossip message");

    // Record outgoing gossip bytes
    metrics.record_gossip_sent(topic_str, data.len());

    match swarm.behaviour_mut().gossipsub.publish(topic, data.clone()) {
        Ok(msg_id) => {
            debug!(topic = %topic_str, %msg_id, msg_bytes = data.len(), "gossip message published");
        }
        Err(e) => {
            // PublishError::InsufficientPeers is expected when starting up with no peers yet
            warn!(topic = %topic_str, error = %e, "gossip publish failed");
        }
    }
}

// Re-export the generated event type for the combined behaviour.
use crate::network::SimBehaviourEvent;
use libp2p::gossipsub;
