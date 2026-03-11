//! State machine: 12-second slot ticker with role-based event logic.
//!
//! The state machine drives the swarm event loop and triggers network broadcasts
//! at the correct phase within each 12-second slot, based on the node's roles.

use crate::metrics::BandwidthMetrics;
use crate::network::{
    SimBehaviour, TOPIC_CL_BEACON_BLOCK, TOPIC_CL_BLOB_SIDECAR, TOPIC_CL_PAYLOAD_ENVELOPE,
    TOPIC_CL_PTC_ATTESTATION,
};
use crate::types::*;

use futures::StreamExt;
use libp2p::gossipsub::IdentTopic;
use libp2p::request_response;
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
    seed: u64,
    num_slots: u64,
    metrics: &mut BandwidthMetrics,
) {
    let mut rng = StdRng::seed_from_u64(seed);
    let node_index = seed; // use seed as a simple unique index for this node

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
        drain_events_until(swarm, roles, &mut rng, slot_start + Duration::from_secs(4), slot, metrics, &mut slot_state).await;

        // ---------------------------------------------------------------
        // t=4-6s — Payload & blob release phase (builder only)
        //
        // By this point the builder has seen the beacon block (containing
        // its bid) and knows it was selected. It publishes:
        //   1. Signed execution payload envelope on CL gossip
        //   2. Blob sidecars on CL gossip
        // Blob hash info is already embedded in the envelope's KZG
        // commitments, so no separate EL announcement is needed.
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
                let sidecar = BlobSidecar::dummy(slot, i);
                publish_gossip(
                    swarm,
                    TOPIC_CL_BLOB_SIDECAR,
                    &GossipMessage::Sidecar(sidecar),
                    metrics,
                );
            }
            info!(slot, blobs = BLOBS_PER_SLOT, "builder: published blob sidecars");
        }

        // Drain events until t=6s
        drain_events_until(swarm, roles, &mut rng, slot_start + Duration::from_secs(6), slot, metrics, &mut slot_state).await;

        // Drain events until t=8s
        drain_events_until(swarm, roles, &mut rng, slot_start + Duration::from_secs(8), slot, metrics, &mut slot_state).await;

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
        drain_events_until(swarm, roles, &mut rng, slot_start + Duration::from_secs(12), slot, metrics, &mut slot_state).await;

        // Emit per-slot bandwidth summary
        metrics.emit_slot_summary(slot);

        info!(slot, "=== SLOT END ===");
    }

    // Emit end-of-simulation summary
    metrics.emit_final_summary(num_slots);

    info!("all slots completed, shutting down");
}

// ---------------------------------------------------------------------------
// Event drain loop
// ---------------------------------------------------------------------------

/// Process swarm events until the given deadline.
///
/// Uses `tokio::select!` to multiplex between incoming swarm events and
/// the deadline timer.
async fn drain_events_until(
    swarm: &mut Swarm<SimBehaviour>,
    roles: &NodeRoles,
    rng: &mut StdRng,
    deadline: Instant,
    current_slot: u64,
    metrics: &mut BandwidthMetrics,
    slot_state: &mut SlotState,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                break;
            }
            event = swarm.select_next_some() => {
                handle_swarm_event(swarm, roles, rng, event, current_slot, metrics, slot_state);
            }
        }
    }
}

/// Handle a single swarm event, dispatching based on the node's persona.
fn handle_swarm_event(
    swarm: &mut Swarm<SimBehaviour>,
    roles: &NodeRoles,
    rng: &mut StdRng,
    event: SwarmEvent<SimBehaviourEvent>,
    current_slot: u64,
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

            // Record bandwidth
            metrics.record_gossip_received(&topic, msg_bytes);

            // Deserialize the wrapper
            if let Ok(msg) = serde_json::from_slice::<GossipMessage>(&message.data) {
                handle_gossip_message(swarm, roles, rng, msg, current_slot, metrics, slot_state);
            } else {
                warn!(%topic, "failed to deserialize gossip message");
            }
        }

        // -- Request-Response: incoming request --
        SwarmEvent::Behaviour(SimBehaviourEvent::ReqRes(
            request_response::Event::Message {
                peer,
                message:
                    request_response::Message::Request {
                        request_id,
                        request,
                        channel,
                    },
                ..
            },
        )) => {
            // Record incoming request bytes
            let req_bytes = serde_json::to_vec(&request).map(|v| v.len()).unwrap_or(0);
            metrics.record_request_received(req_bytes);

            debug!(%peer, %request_id, req_bytes, "req-res request received");
            handle_incoming_request(swarm, roles, request, channel, metrics);
        }

        // -- Request-Response: incoming response --
        SwarmEvent::Behaviour(SimBehaviourEvent::ReqRes(
            request_response::Event::Message {
                peer,
                message:
                    request_response::Message::Response {
                        request_id,
                        response,
                    },
                ..
            },
        )) => {
            // Record incoming response bytes
            let resp_bytes = serde_json::to_vec(&response).map(|v| v.len()).unwrap_or(0);
            metrics.record_response_received(resp_bytes);

            debug!(%peer, %request_id, resp_bytes, "req-res response received");
            handle_incoming_response(response);
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

// ---------------------------------------------------------------------------
// Gossip message handling
// ---------------------------------------------------------------------------

fn handle_gossip_message(
    swarm: &mut Swarm<SimBehaviour>,
    roles: &NodeRoles,
    rng: &mut StdRng,
    msg: GossipMessage,
    current_slot: u64,
    metrics: &mut BandwidthMetrics,
    slot_state: &mut SlotState,
) {
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

        GossipMessage::BlobHash(announce) => {
            info!(
                slot = announce.slot,
                hashes = announce.blob_hashes.len(),
                "received blob hash announce"
            );

            // TODO: BlobHashAnnounce is not currently published by any role.
            // In a future iteration, samplers should react by fetching only
            // their custody column cells, and providers should fetch the
            // entire blob payload.
            if roles.is_sampler() {
                send_custody_requests(swarm, rng, &announce, current_slot, metrics);
            }
            if roles.is_provider() {
                send_full_payload_requests(swarm, &announce, current_slot, metrics);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// EL request/response handling
// ---------------------------------------------------------------------------

/// Sampler: send custody cell requests for a subset of columns + 1 random extra.
fn send_custody_requests(
    swarm: &mut Swarm<SimBehaviour>,
    rng: &mut StdRng,
    announce: &BlobHashAnnounce,
    _current_slot: u64,
    metrics: &mut BandwidthMetrics,
) {
    // Deterministically choose custody columns
    let mut columns: HashSet<u64> = HashSet::new();
    while columns.len() < CUSTODY_SUBSET_SIZE {
        columns.insert(rng.gen_range(0..NUM_CUSTODY_COLUMNS));
    }
    // Add 1 random extra column
    let extra: u64 = rng.gen_range(0..NUM_CUSTODY_COLUMNS);
    columns.insert(extra);

    let column_indices: Vec<u64> = columns.into_iter().collect();

    for blob_hash in &announce.blob_hashes {
        let request = SimRequest::CustodyCell(CustodyCellRequest {
            slot: announce.slot,
            blob_hash: *blob_hash,
            column_indices: column_indices.clone(),
        });

        // Measure request size
        let req_bytes = serde_json::to_vec(&request).map(|v| v.len()).unwrap_or(0);

        // Send to all connected peers (in practice, to the builder or other holders)
        let peers: Vec<_> = swarm.connected_peers().cloned().collect();
        for peer_id in peers {
            info!(
                slot = announce.slot,
                columns = ?column_indices,
                %peer_id,
                req_bytes,
                "sampler: sending custody cell request"
            );
            metrics.record_request_sent(req_bytes);
            swarm
                .behaviour_mut()
                .req_res
                .send_request(&peer_id, request.clone());
        }
    }
}

/// Provider: send full payload requests.
fn send_full_payload_requests(
    swarm: &mut Swarm<SimBehaviour>,
    announce: &BlobHashAnnounce,
    _current_slot: u64,
    metrics: &mut BandwidthMetrics,
) {
    for blob_hash in &announce.blob_hashes {
        let request = SimRequest::FullPayload(FullPayloadRequest {
            slot: announce.slot,
            blob_hash: *blob_hash,
        });

        // Measure request size
        let req_bytes = serde_json::to_vec(&request).map(|v| v.len()).unwrap_or(0);

        let peers: Vec<_> = swarm.connected_peers().cloned().collect();
        for peer_id in peers {
            info!(
                slot = announce.slot,
                %peer_id,
                req_bytes,
                "provider: sending full payload request"
            );
            metrics.record_request_sent(req_bytes);
            swarm
                .behaviour_mut()
                .req_res
                .send_request(&peer_id, request.clone());
        }
    }
}

/// Handle an incoming request (typically on the builder side).
fn handle_incoming_request(
    swarm: &mut Swarm<SimBehaviour>,
    _roles: &NodeRoles,
    request: SimRequest,
    channel: request_response::ResponseChannel<SimResponse>,
    metrics: &mut BandwidthMetrics,
) {
    let response = match request {
        SimRequest::CustodyCell(req) => {
            info!(
                slot = req.slot,
                columns = ?req.column_indices,
                "handling custody cell request"
            );
            let cells: Vec<(u64, Vec<u8>)> = req
                .column_indices
                .iter()
                .map(|&col| (col, vec![0xAA; 64])) // dummy cell data
                .collect();
            SimResponse::CustodyCell(CustodyCellResponse {
                slot: req.slot,
                blob_hash: req.blob_hash,
                cells,
            })
        }
        SimRequest::FullPayload(req) => {
            info!(slot = req.slot, "handling full payload request");
            SimResponse::FullPayload(FullPayloadResponse {
                slot: req.slot,
                blob_hash: req.blob_hash,
                payload_data: vec![0xBB; DUMMY_BLOB_SIZE],
            })
        }
    };

    // Record outgoing response bytes
    let resp_bytes = serde_json::to_vec(&response).map(|v| v.len()).unwrap_or(0);
    metrics.record_response_sent(resp_bytes);
    debug!(resp_bytes, "req-res response sent");

    if swarm
        .behaviour_mut()
        .req_res
        .send_response(channel, response)
        .is_err()
    {
        warn!("failed to send response (channel closed)");
    }
}

/// Handle an incoming response (Sampler or Provider side).
fn handle_incoming_response(response: SimResponse) {
    match response {
        SimResponse::CustodyCell(resp) => {
            info!(
                slot = resp.slot,
                cells = resp.cells.len(),
                "sampler: received custody cells"
            );
        }
        SimResponse::FullPayload(resp) => {
            info!(
                slot = resp.slot,
                payload_size = resp.payload_data.len(),
                "provider: received full payload"
            );
        }
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
