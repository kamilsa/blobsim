//! Bandwidth metrics for verifying EIP-8070's bandwidth reduction claim.
//!
//! Tracks per-slot and cumulative byte counts for EL and CL traffic,
//! split by sent/received. Emits structured log lines parseable by
//! Shadow post-processing scripts.

use crate::types::NodeRoles;
use tracing::info;

/// Layer (`cl`/`el`) a gossip topic belongs to, for the §5 traffic breakdown.
fn gossip_layer(topic: &str) -> &'static str {
    if topic.starts_with("/el/") {
        "el"
    } else {
        "cl"
    }
}

/// Coarse message kind for a gossip topic (§5 traffic breakdown).
fn gossip_kind(topic: &str) -> &'static str {
    if topic.contains("beacon_block") {
        "block"
    } else if topic.contains("payload_envelope") {
        "envelope"
    } else if topic.contains("blob_sidecar") {
        "blob_sidecar"
    } else if topic.contains("data_column_sidecar") {
        "data_column"
    } else {
        "other"
    }
}

/// Per-node bandwidth counters.
pub struct BandwidthMetrics {
    roles_label: String,
    /// Current slot, stamped onto per-message `traffic` events (§5).
    current_slot: u64,

    // -- Per-slot counters (reset each slot) --
    el_bytes_sent: u64,
    el_bytes_received: u64,
    cl_bytes_sent: u64,
    cl_bytes_received: u64,
    el_requests_sent: u64,
    el_responses_received: u64,
    el_requests_received: u64,
    el_responses_sent: u64,
    el_announces_sent: u64,
    el_announces_received: u64,
    gossip_messages_sent: u64,
    gossip_messages_received: u64,
    gossip_messages_forwarded: u64,

    // -- Partial data-column counters (gossipsub 1.3 cell-level deltas) --
    /// Partial messages received via `Event::Partial` (headers + deltas).
    partial_messages_received: u64,
    /// Of those, how many carried a header (phase-1 header-only messages).
    partial_headers_received: u64,
    /// New cells merged in from received partials.
    partial_cells_received: u64,
    /// Data columns published/seeded via the partial protocol.
    partial_columns_published: u64,
    /// Columns that assembled to completion this slot.
    partial_columns_completed: u64,
    /// Custody cells received over EL and stored as partial blob data in the
    /// local EL blob pool (the getBlobsV4 sparse-blobpool path).
    partial_cells_pooled: u64,

    // -- Cumulative totals --
    total_el_bytes_sent: u64,
    total_el_bytes_received: u64,
    total_cl_bytes_sent: u64,
    total_cl_bytes_received: u64,
}

impl BandwidthMetrics {
    /// Create a new metrics tracker for the given roles.
    pub fn new(roles: &NodeRoles) -> Self {
        Self {
            roles_label: roles.to_string(),
            current_slot: 0,
            el_bytes_sent: 0,
            el_bytes_received: 0,
            cl_bytes_sent: 0,
            cl_bytes_received: 0,
            el_requests_sent: 0,
            el_responses_received: 0,
            el_requests_received: 0,
            el_responses_sent: 0,
            el_announces_sent: 0,
            el_announces_received: 0,
            gossip_messages_sent: 0,
            gossip_messages_received: 0,
            gossip_messages_forwarded: 0,
            partial_messages_received: 0,
            partial_headers_received: 0,
            partial_cells_received: 0,
            partial_columns_published: 0,
            partial_columns_completed: 0,
            partial_cells_pooled: 0,
            total_el_bytes_sent: 0,
            total_el_bytes_received: 0,
            total_cl_bytes_sent: 0,
            total_cl_bytes_received: 0,
        }
    }

    // -- Recording helpers --

    /// Set the slot stamped onto subsequent per-message `traffic` events. Call at
    /// the start of each slot.
    pub fn set_slot(&mut self, slot: u64) {
        self.current_slot = slot;
    }

    /// Emit one per-message `traffic` event (§5 intra-slot bandwidth). The message
    /// kind is emitted as `mkind` (not `kind`) to avoid colliding with the reserved
    /// `kind=` token that names the event type.
    fn emit_traffic(&self, layer: &str, dir: &str, mkind: &str, bytes: usize) {
        crate::event!(
            "traffic",
            self.current_slot,
            layer = layer,
            dir = dir,
            mkind = mkind,
            bytes = bytes
        );
    }

    /// Record a gossip message sent. Routes to CL or EL based on topic prefix.
    pub fn record_gossip_sent(&mut self, topic: &str, bytes: usize) {
        if topic.starts_with("/el/") {
            self.el_bytes_sent += bytes as u64;
        } else {
            self.cl_bytes_sent += bytes as u64;
        }
        self.gossip_messages_sent += 1;
        self.emit_traffic(gossip_layer(topic), "out", gossip_kind(topic), bytes);
    }

    /// Record a gossip message received. Routes to CL or EL based on topic prefix.
    pub fn record_gossip_received(&mut self, topic: &str, bytes: usize) {
        if topic.starts_with("/el/") {
            self.el_bytes_received += bytes as u64;
        } else {
            self.cl_bytes_received += bytes as u64;
        }
        self.gossip_messages_received += 1;
        self.emit_traffic(gossip_layer(topic), "in", gossip_kind(topic), bytes);
    }

    /// Record a gossip message forwarded to mesh peers (outgoing bandwidth).
    /// Routes to CL or EL based on topic prefix.
    pub fn record_gossip_forwarded(&mut self, topic: &str, bytes: usize) {
        if topic.starts_with("/el/") {
            self.el_bytes_sent += bytes as u64;
        } else {
            self.cl_bytes_sent += bytes as u64;
        }
        self.gossip_messages_forwarded += 1;
        self.emit_traffic(gossip_layer(topic), "out", gossip_kind(topic), bytes);
    }

    /// Record an EL request-response request sent (requesting peer → holder).
    pub fn record_request_sent(&mut self, bytes: usize) {
        self.el_bytes_sent += bytes as u64;
        self.el_requests_sent += 1;
        self.emit_traffic("el", "out", "el_request", bytes);
    }

    /// Record an EL request-response response received (holder → requesting peer).
    pub fn record_response_received(&mut self, bytes: usize) {
        self.el_bytes_received += bytes as u64;
        self.el_responses_received += 1;
        self.emit_traffic("el", "in", "el_response", bytes);
    }

    /// Record an EL request-response request received (peer → Builder).
    pub fn record_request_received(&mut self, bytes: usize) {
        self.el_bytes_received += bytes as u64;
        self.el_requests_received += 1;
        self.emit_traffic("el", "in", "el_request", bytes);
    }

    /// Record an EL request-response response sent (Builder → peer).
    pub fn record_response_sent(&mut self, bytes: usize) {
        self.el_bytes_sent += bytes as u64;
        self.el_responses_sent += 1;
        self.emit_traffic("el", "out", "el_response", bytes);
    }

    /// Record an EL blob-hash announcement sent (Builder → peer). Call once per
    /// recipient peer so fan-out bandwidth is accounted (mirrors gossip forwarding).
    pub fn record_el_announce_sent(&mut self, bytes: usize) {
        self.el_bytes_sent += bytes as u64;
        self.el_announces_sent += 1;
        self.emit_traffic("el", "out", "announce", bytes);
    }

    /// Record an EL blob-hash announcement received.
    pub fn record_el_announce_received(&mut self, bytes: usize) {
        self.el_bytes_received += bytes as u64;
        self.el_announces_received += 1;
        self.emit_traffic("el", "in", "announce", bytes);
    }

    /// Record a partial data-column message received via `Event::Partial`. The
    /// payload + metadata bytes count as inbound CL bandwidth; `new_cells` is how
    /// many previously-missing cells it delivered; `has_header` flags a phase-1
    /// header-only message.
    pub fn record_partial_received(&mut self, bytes: usize, new_cells: usize, has_header: bool) {
        self.cl_bytes_received += bytes as u64;
        self.partial_messages_received += 1;
        self.partial_cells_received += new_cells as u64;
        if has_header {
            self.partial_headers_received += 1;
        }
        self.emit_traffic("cl", "in", "partial_column", bytes);
    }

    /// Record that we published/seeded a data column via the partial protocol.
    pub fn record_partial_column_published(&mut self) {
        self.partial_columns_published += 1;
    }

    /// Record that a data column assembled to completion.
    pub fn record_partial_column_completed(&mut self) {
        self.partial_columns_completed += 1;
    }

    /// Record custody cells received over EL and stored as partial blob data in
    /// the local EL blob pool.
    pub fn record_partial_cells_pooled(&mut self, cells: usize) {
        self.partial_cells_pooled += cells as u64;
    }

    // -- Reporting --

    /// Emit a structured per-slot summary log line and reset slot counters.
    pub fn emit_slot_summary(&mut self, slot: u64) {
        info!(
            target: "metrics",
            "METRIC slot={} roles={} el_bytes_sent={} el_bytes_received={} \
             cl_bytes_sent={} cl_bytes_received={} \
             el_requests_sent={} el_responses_received={} \
             el_requests_received={} el_responses_sent={} \
             el_announces_sent={} el_announces_received={} \
             gossip_sent={} gossip_received={} gossip_forwarded={} \
             partial_msgs_received={} partial_headers_received={} \
             partial_cells_received={} partial_columns_published={} \
             partial_columns_completed={} partial_cells_pooled={}",
            slot,
            self.roles_label,
            self.el_bytes_sent,
            self.el_bytes_received,
            self.cl_bytes_sent,
            self.cl_bytes_received,
            self.el_requests_sent,
            self.el_responses_received,
            self.el_requests_received,
            self.el_responses_sent,
            self.el_announces_sent,
            self.el_announces_received,
            self.gossip_messages_sent,
            self.gossip_messages_received,
            self.gossip_messages_forwarded,
            self.partial_messages_received,
            self.partial_headers_received,
            self.partial_cells_received,
            self.partial_columns_published,
            self.partial_columns_completed,
            self.partial_cells_pooled,
        );

        // Accumulate into totals
        self.total_el_bytes_sent += self.el_bytes_sent;
        self.total_el_bytes_received += self.el_bytes_received;
        self.total_cl_bytes_sent += self.cl_bytes_sent;
        self.total_cl_bytes_received += self.cl_bytes_received;

        // Reset per-slot counters
        self.el_bytes_sent = 0;
        self.el_bytes_received = 0;
        self.cl_bytes_sent = 0;
        self.cl_bytes_received = 0;
        self.el_requests_sent = 0;
        self.el_responses_received = 0;
        self.el_requests_received = 0;
        self.el_responses_sent = 0;
        self.el_announces_sent = 0;
        self.el_announces_received = 0;
        self.gossip_messages_sent = 0;
        self.gossip_messages_received = 0;
        self.gossip_messages_forwarded = 0;
        self.partial_messages_received = 0;
        self.partial_headers_received = 0;
        self.partial_cells_received = 0;
        self.partial_columns_published = 0;
        self.partial_columns_completed = 0;
        self.partial_cells_pooled = 0;
    }

    /// Emit a structured end-of-simulation summary log line.
    pub fn emit_final_summary(&self, num_slots: u64) {
        let avg_el_sent = if num_slots > 0 {
            self.total_el_bytes_sent / num_slots
        } else {
            0
        };
        let avg_el_recv = if num_slots > 0 {
            self.total_el_bytes_received / num_slots
        } else {
            0
        };
        let avg_cl_sent = if num_slots > 0 {
            self.total_cl_bytes_sent / num_slots
        } else {
            0
        };
        let avg_cl_recv = if num_slots > 0 {
            self.total_cl_bytes_received / num_slots
        } else {
            0
        };

        info!(
            target: "metrics",
            "SUMMARY roles={} slots={} \
             total_el_bytes_sent={} total_el_bytes_received={} \
             total_cl_bytes_sent={} total_cl_bytes_received={} \
             avg_el_sent_per_slot={} avg_el_recv_per_slot={} \
             avg_cl_sent_per_slot={} avg_cl_recv_per_slot={}",
            self.roles_label,
            num_slots,
            self.total_el_bytes_sent,
            self.total_el_bytes_received,
            self.total_cl_bytes_sent,
            self.total_cl_bytes_received,
            avg_el_sent,
            avg_el_recv,
            avg_cl_sent,
            avg_cl_recv,
        );
    }
}
