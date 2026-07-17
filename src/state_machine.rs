//! State machine: 12-second slot ticker with role-based event logic.
//!
//! The state machine drives both networking layers and triggers broadcasts at the
//! correct phase within each 12-second slot:
//!   - CL gossip (beacon block proposals, payload envelopes, blob sidecars, data
//!     columns) over the libp2p/QUIC swarm (`network.rs`).
//!   - EL blob propagation (announce → request → serve) over the TCP layer
//!     (`el_net.rs`).

use crate::el_net::{ElEvent, ElHandle, ElPeerId};
use crate::metrics::BandwidthMetrics;
use crate::network::{
    data_column_topic, subnet_for_column, subnet_from_topic, SimBehaviour, TOPIC_CL_BEACON_BLOCK,
    TOPIC_CL_BLOB_SIDECAR, TOPIC_CL_PAYLOAD_ENVELOPE,
};
use crate::partial::{
    EligibleRows, OutgoingPartialColumn, PartialColumnAssembler, PartialColumnHeaderTracker,
    ReconstructionApply, PARTIAL_COLUMNS_VERSION_BYTE,
};
use crate::types::*;

use alloy_rlp::Bytes;
use futures::StreamExt;
use libp2p::gossipsub::{self, IdentTopic};
use libp2p::swarm::SwarmEvent;
use libp2p::Swarm;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// How many blocks the partial-column assembler and header tracker retain.
/// The tracker must retain at least as many blocks as can be republished
/// (assembler capacity): evicting a block's header-sent set while its columns
/// can still be re-advertised would re-send phase-1 header messages to every
/// peer on each republish.
const ASSEMBLER_CAPACITY: usize = 16;
const HEADER_TRACKER_CAPACITY: usize = ASSEMBLER_CAPACITY;
/// How many pending blobs the local EL blob pool retains (oldest evicted first).
const EL_BLOB_POOL_CAPACITY: usize = 64;
/// How many slots a blob is remembered as already-included, so it is neither
/// re-pooled nor re-included after a block commits to it (mempool eviction window).
const INCLUDED_WINDOW_SLOTS: u64 = 4;
/// Per-announced-blob probability that a non-builder CL node fetches custody
/// cells instead of the full payload.
const SAMPLER_FETCH_PROBABILITY: f64 = 0.85;
/// Hard ceiling on blob rows waiting for delayed reconstruction. Normal runs
/// stay far below this; the bound protects extreme delays or malformed blocks.
const MAX_SCHEDULED_RECONSTRUCTION_ROWS: usize = ASSEMBLER_CAPACITY * 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobReconstructionConfig {
    pub delay: Duration,
    pub trigger: BlobReconstructionTrigger,
}

#[derive(Debug, Clone)]
struct ScheduledReconstruction {
    block_root: [u8; 32],
    generation: u64,
    attempt: u64,
    trigger: BlobReconstructionTrigger,
    rows: Vec<usize>,
    ready_at: Instant,
}

/// A pooled entry for one announced blob hash: either the full blob, or a sparse
/// set of custody cells (column index → 2 KiB cell).
///
/// The two variants are **mutually exclusive per hash**: the fetch policy makes
/// exactly one decision per blob (sampler XOR provider, see
/// [`send_random_sparse_blobpool_requests`]) and each hash is announced to a node
/// once, so a blob is fetched as *either* full *or* partial data — never both.
/// [`insert_full`](ElBlobPool::insert_full) and
/// [`insert_cells`](ElBlobPool::insert_cells) `debug_assert!` this invariant
/// rather than reconcile a full/partial mix that cannot arise.
enum ElBlobEntry {
    /// The full blob (128 KiB); any column's cell is derivable on demand.
    Full(Vec<u8>),
    /// Individually-received custody cells, keyed by column index. `BTreeMap`
    /// keeps the column set deterministically ordered (tidy logs; free).
    Partial(BTreeMap<u64, Cell>),
}

/// The local EL blobpool: blob data this node's EL received over EL networking,
/// keyed by announced blob hash. Models EIP-8070's sparse blobpool — an entry is
/// either the full blob (provider/full-payload pulls) or just the custody cells
/// we sampled ([`ElBlobEntry`]).
///
/// It has two parts:
///   - `pending`: entries held but not yet seen in any block. A **builder** takes
///     up to a per-block cap of *full* blobs from here to build a block (their
///     hashes become the block's commitments); a **partial-column node** reads it
///     via [`get_cells`](ElBlobPool::get_cells) (the `engine_getBlobsV4` analog)
///     to derive the custody cells it holds. Bounded to [`EL_BLOB_POOL_CAPACITY`]
///     entries, FIFO — a partial entry counts as one slot, same as a full blob.
///   - `included`: hash → slot for blobs seen included in a block. Once here a
///     blob is evicted from `pending` and refused re-entry (as full or partial),
///     so it can never be re-included across slots. Pruned to the last
///     [`INCLUDED_WINDOW_SLOTS`].
///
/// Filled only by the EL receiving payloads/cells over EL networking — never by a
/// CL request.
#[derive(Default)]
struct ElBlobPool {
    pending: std::collections::VecDeque<([u8; 32], ElBlobEntry)>,
    included: std::collections::HashMap<[u8; 32], u64>,
}

impl ElBlobPool {
    /// Locate the entry for `hash`, if pooled.
    fn entry(&self, hash: &[u8; 32]) -> Option<&ElBlobEntry> {
        self.pending.iter().find(|(h, _)| h == hash).map(|(_, e)| e)
    }

    fn entry_mut(&mut self, hash: &[u8; 32]) -> Option<&mut ElBlobEntry> {
        self.pending
            .iter_mut()
            .find(|(h, _)| h == hash)
            .map(|(_, e)| e)
    }

    /// Whether any data (full or partial) is pooled for `hash`.
    fn contains(&self, hash: &[u8; 32]) -> bool {
        self.entry(hash).is_some()
    }

    /// Pool the full blob for `hash`. Refused if the hash was already included in a
    /// block, or already held as a full blob. A hash held as partial cells should
    /// never also arrive as a full blob (see [`ElBlobEntry`]); that is asserted.
    fn insert_full(&mut self, hash: [u8; 32], blob: Vec<u8>) {
        if self.included.contains_key(&hash) {
            return;
        }
        match self.entry_mut(&hash) {
            Some(ElBlobEntry::Full(_)) => {} // dedup: already have the full blob
            Some(slot @ ElBlobEntry::Partial(_)) => {
                // Invariant violation: this hash was fetched as partial cells, yet
                // a full blob arrived for it. Surfaces a broken fetch policy in
                // debug; upgrade in place as a safe release fallback.
                debug_assert!(
                    false,
                    "insert_full for a hash already held as partial cells"
                );
                *slot = ElBlobEntry::Full(blob);
            }
            None => {
                self.pending.push_back((hash, ElBlobEntry::Full(blob)));
                while self.pending.len() > EL_BLOB_POOL_CAPACITY {
                    self.pending.pop_front();
                }
            }
        }
    }

    /// Merge custody cells for `hash` into the pool. Returns how many
    /// previously-absent cells were added (0 ⇒ no growth). Cells merge into the
    /// existing partial entry in place (no duplicate hash, FIFO position kept).
    /// Refused for already-included hashes. A hash held as a full blob should never
    /// also arrive as custody cells (see [`ElBlobEntry`]); that is asserted.
    fn insert_cells(
        &mut self,
        hash: [u8; 32],
        cells: impl IntoIterator<Item = (u64, Cell)>,
    ) -> usize {
        if self.included.contains_key(&hash) {
            return 0;
        }
        match self.entry_mut(&hash) {
            Some(ElBlobEntry::Full(_)) => {
                // Invariant violation: this hash was fetched as a full blob, yet
                // custody cells arrived for it. Surfaces a broken fetch policy in
                // debug; ignore them as a safe release fallback (the full blob
                // already covers every column).
                debug_assert!(false, "insert_cells for a hash already held as a full blob");
                0
            }
            Some(ElBlobEntry::Partial(map)) => {
                let mut added = 0;
                for (col, cell) in cells {
                    if map.insert(col, cell).is_none() {
                        added += 1;
                    }
                }
                added // in place: no push_back, FIFO order untouched
            }
            None => {
                let map: BTreeMap<u64, Cell> = cells.into_iter().collect();
                let added = map.len();
                if added == 0 {
                    return 0;
                }
                self.pending.push_back((hash, ElBlobEntry::Partial(map)));
                while self.pending.len() > EL_BLOB_POOL_CAPACITY {
                    self.pending.pop_front();
                }
                added
            }
        }
    }

    /// Local `engine_getBlobsV4` read: for `hash` and a set of column indices,
    /// return one `Option<Cell>` per requested index, in order. A full entry
    /// derives every cell; a partial entry returns the stored cell if present else
    /// `None` (the EL lacks it); no entry ⇒ all `None`. Never a network call.
    fn get_cells(&self, hash: &[u8; 32], columns: &[u64]) -> Vec<Option<Cell>> {
        match self.entry(hash) {
            Some(ElBlobEntry::Full(blob)) => columns
                .iter()
                .map(|&c| Some(derive_cell(blob, c)))
                .collect(),
            Some(ElBlobEntry::Partial(map)) => {
                columns.iter().map(|&c| map.get(&c).cloned()).collect()
            }
            None => vec![None; columns.len()],
        }
    }

    /// Record `hash` as included in a block at `slot`: remember it for the
    /// eviction window and drop it from `pending`. Idempotent.
    fn mark_included(&mut self, hash: [u8; 32], slot: u64) {
        self.included.insert(hash, slot);
        self.pending.retain(|(h, _)| h != &hash);
    }

    /// Take up to `max` not-yet-included *full* blobs for a block at `slot`,
    /// marking each included (so it is never re-included) and removing it from
    /// `pending`. Partial entries are skipped (left in place) — a builder cannot
    /// block-include a blob it holds only cells of. Any remainder stays pending.
    fn take_pending(&mut self, max: usize, slot: u64) -> Vec<([u8; 32], Vec<u8>)> {
        let mut taken = Vec::new();
        let mut i = 0;
        while taken.len() < max && i < self.pending.len() {
            if matches!(self.pending[i].1, ElBlobEntry::Full(_)) {
                let (hash, entry) = self.pending.remove(i).expect("index in bounds");
                let ElBlobEntry::Full(blob) = entry else {
                    unreachable!("guarded by matches! above")
                };
                self.included.insert(hash, slot);
                taken.push((hash, blob));
            } else {
                i += 1; // skip partial entry, keep scanning
            }
        }
        taken
    }

    /// Forget inclusions older than [`INCLUDED_WINDOW_SLOTS`] relative to `now`.
    fn prune_included(&mut self, now: u64) {
        self.included
            .retain(|_, &mut slot| now.saturating_sub(slot) < INCLUDED_WINDOW_SLOTS);
    }
}

/// Node-local state for the gossipsub 1.3 partial-column path. Inert unless
/// `--enable-partial-columns` is set (except `custody_columns`, which also
/// drives EL sparse-blobpool custody-cell requests).
struct PartialState {
    enabled: bool,
    /// Whether the CL may read blobs from the local EL blob pool to derive its
    /// custody columns (the `engine_getBlobs` analog). When false, a node relies
    /// solely on cells arriving from peers over CL (cell-level deltas).
    get_blobs_enabled: bool,
    /// This node's custody columns: the cells its EL requests from the sparse
    /// blobpool, and the column subset it derives/requests on the CL side.
    custody_columns: Vec<u64>,
    assembler: PartialColumnAssembler,
    header_tracker: PartialColumnHeaderTracker,
    /// Blocks whose custody columns we have already advertised/published once.
    custody_advertised_blocks: HashSet<[u8; 32]>,
    /// Slots for which a `custody_complete` event has already been emitted, so the
    /// §4 completion transition fires at most once per slot.
    custody_complete_slots: HashSet<u64>,
    reconstruction: Option<BlobReconstructionConfig>,
    scheduled_reconstructions: Vec<ScheduledReconstruction>,
    next_reconstruction_attempt: u64,
}

impl PartialState {
    fn new(
        enabled: bool,
        get_blobs_enabled: bool,
        seed: u64,
        custody_columns: usize,
        reconstruction: Option<BlobReconstructionConfig>,
    ) -> Self {
        Self {
            enabled,
            get_blobs_enabled,
            custody_columns: custody_columns_for_seed(seed, custody_columns),
            assembler: PartialColumnAssembler::new(ASSEMBLER_CAPACITY),
            header_tracker: PartialColumnHeaderTracker::new(HEADER_TRACKER_CAPACITY),
            custody_advertised_blocks: HashSet::new(),
            custody_complete_slots: HashSet::new(),
            reconstruction,
            scheduled_reconstructions: Vec::new(),
            next_reconstruction_attempt: 1,
        }
    }

    fn schedule_eligible(&mut self, block_root: [u8; 32]) {
        let Some(config) = self.reconstruction else {
            return;
        };
        let Some(batch) = self
            .assembler
            .take_eligible_rows(block_root, config.trigger)
        else {
            return;
        };
        self.schedule_batch(batch, config);
    }

    fn schedule_batch(&mut self, mut batch: EligibleRows, config: BlobReconstructionConfig) {
        let attempt = self.next_reconstruction_attempt;
        self.next_reconstruction_attempt = self.next_reconstruction_attempt.wrapping_add(1).max(1);
        let queued_rows: usize = self
            .scheduled_reconstructions
            .iter()
            .map(|scheduled| scheduled.rows.len())
            .sum();
        let available = MAX_SCHEDULED_RECONSTRUCTION_ROWS.saturating_sub(queued_rows);
        let dropped = batch.rows.split_off(batch.rows.len().min(available));
        if !dropped.is_empty() {
            let rows: Vec<usize> = dropped.iter().map(|(row, _)| *row).collect();
            self.assembler
                .release_reconstructing(batch.block_root, batch.generation, &rows);
            for row in rows {
                event!(
                    "blob_reconstruction_dropped",
                    slot_for_block_root(&batch.block_root),
                    blob_index = row,
                    generation = batch.generation,
                    attempt = attempt,
                    trigger = batch.trigger,
                    reason = "queue-capacity"
                );
            }
        }
        if batch.rows.is_empty() {
            return;
        }

        let start = Instant::now();
        for &(blob_index, cells_held) in &batch.rows {
            event!(
                "blob_reconstruction_started",
                slot_for_block_root(&batch.block_root),
                blob_index = blob_index,
                generation = batch.generation,
                attempt = attempt,
                trigger = batch.trigger,
                cells_held = cells_held,
                complete_columns = batch.complete_columns,
                delay_ms = config.delay.as_millis()
            );
        }
        self.scheduled_reconstructions
            .push(ScheduledReconstruction {
                block_root: batch.block_root,
                generation: batch.generation,
                attempt,
                trigger: batch.trigger,
                rows: batch.rows.into_iter().map(|(row, _)| row).collect(),
                ready_at: start + config.delay,
            });
        self.scheduled_reconstructions.sort_by_key(|batch| {
            (
                batch.ready_at,
                batch.block_root,
                batch.generation,
                batch.attempt,
                batch.rows.first().copied().unwrap_or(0),
            )
        });
    }

    fn retry_eligible(&mut self) {
        for block_root in self.assembler.blocks_with_header() {
            self.schedule_eligible(block_root);
        }
    }

    fn drop_pending_reconstructions(&mut self, reason: &'static str) {
        for batch in self.scheduled_reconstructions.drain(..) {
            for blob_index in batch.rows {
                event!(
                    "blob_reconstruction_dropped",
                    slot_for_block_root(&batch.block_root),
                    blob_index = blob_index,
                    generation = batch.generation,
                    attempt = batch.attempt,
                    trigger = batch.trigger,
                    reason = reason
                );
            }
        }
    }
}

/// Emit a `custody_complete` event the first time this node's full custody set has
/// assembled for a block (§4 fetch-completion timing). Guarded to fire once per slot.
fn maybe_emit_custody_complete(partial_state: &mut PartialState, block_root: [u8; 32]) {
    let slot = slot_for_block_root(&block_root);
    if partial_state.custody_complete_slots.contains(&slot) {
        return;
    }
    if partial_state
        .assembler
        .custody_set_complete(&block_root, &partial_state.custody_columns)
    {
        partial_state.custody_complete_slots.insert(slot);
        event!("custody_complete", slot);
    }
}

/// Run the node's main loop for `num_slots` slots.
#[allow(clippy::too_many_arguments)]
pub async fn run_node(
    roles: &NodeRoles,
    swarm: &mut Swarm<SimBehaviour>,
    el: &mut ElHandle,
    seed: u64,
    num_slots: u64,
    metrics: &mut BandwidthMetrics,
    enable_partial_columns: bool,
    disable_get_blobs: bool,
    exec_payload_size: usize,
    blocks_in_blobs: bool,
    custody_columns: usize,
    max_blobs_per_block: usize,
    reconstruction: Option<BlobReconstructionConfig>,
) {
    let mut rng = StdRng::seed_from_u64(seed);
    let node_index = seed; // use seed as a simple unique index for this node

    // Number of connected EL/TCP peers, kept up to date from EL events. Used to
    // account fan-out bandwidth when the builder announces blob hashes.
    let mut el_peer_count: usize = 0;

    // Partial data-column state (gossipsub 1.3 cell-level deltas). Inert unless
    // `--enable-partial-columns` is set.
    let mut partial_state = PartialState::new(
        enable_partial_columns,
        !disable_get_blobs,
        seed,
        custody_columns,
        reconstruction,
    );

    // Full blobs this node's EL receives over EL networking. Builders drain it
    // to assemble a block's blob set; partial-column nodes read it via the
    // local getBlobs analog.
    let mut el_blob_pool = ElBlobPool::default();

    info!(%roles, num_slots, enable_partial_columns, "starting slot ticker");

    for slot in 0..num_slots {
        info!(slot, %roles, "=== SLOT START ===");
        event!("slot_start", slot);
        metrics.set_slot(slot);
        let slot_start = Instant::now();

        // Forget inclusions older than the eviction window.
        el_blob_pool.prune_included(slot);

        // The builder samples one random execution payload per slot. When
        // blocks-in-blobs is enabled, the same bytes are also encoded into
        // payload-blobs whose commitments come first in the block. These blobs are
        // generated locally by the builder and never announced over EL, so peers
        // cannot pre-fill their cells from the EL blob pool.
        let (execution_payload, payload_blobs) = if blocks_in_blobs && roles.is_builder() {
            recoverable_payload_blobs(exec_payload_size, &mut rng)
        } else if roles.is_builder() {
            (random_bytes(&mut rng, exec_payload_size), Vec::new())
        } else {
            (Vec::new(), Vec::new())
        };
        let n_payload = payload_blobs.len();

        // Builder: select up to the remaining budget of not-yet-included blobs from
        // its EL pool to include in this slot's block; `take_pending` marks them
        // included (so they are never re-included) and leaves any overflow pooled
        // for a later slot. Builders never generate EL blob data themselves.
        let el_blobs: Vec<([u8; 32], Vec<u8>)> = if roles.is_builder() {
            el_blob_pool.take_pending(max_blobs_per_block.saturating_sub(n_payload), slot)
        } else {
            Vec::new()
        };

        // Payload-blobs are prepended so their commitments occupy the first
        // `n_payload` slots of `blob_kzg_commitments` (EIP-8142). Each uses a fresh
        // random hash that no peer has seen over EL.
        let mut block_blobs: Vec<([u8; 32], Vec<u8>)> =
            Vec::with_capacity(n_payload + el_blobs.len());
        block_blobs.extend(payload_blobs);
        block_blobs.extend(el_blobs);

        // Commitments naming exactly the block blobs, shared by the t=0 proposal
        // and the t=4-6 payload reveal.
        let commitments: Vec<Vec<u8>> = block_blobs
            .iter()
            .map(|(hash, _)| commitment_for_blob_hash(hash))
            .collect();

        // ---------------------------------------------------------------
        // t=0s — Proposal phase (proposer == builder in this model)
        //
        // The proposal commits to the block's blobs. Validators that already hold
        // EL-propagated blobs (matched by hash against their own EL pool) start
        // propagating those columns on seeing it; builder-local payload-blobs have
        // no matching EL pool entries.
        // ---------------------------------------------------------------
        if roles.is_proposer() {
            let block = SignedBeaconBlock::with_commitments(slot, node_index, commitments.clone());
            publish_gossip(
                swarm,
                TOPIC_CL_BEACON_BLOCK,
                &GossipMessage::BeaconBlock(block),
                metrics,
            );
            info!(
                slot,
                blobs = block_blobs.len(),
                payload_blobs = n_payload,
                "proposer: published beacon block proposal with blob commitments"
            );
            let block_hashes: Vec<[u8; 32]> = block_blobs.iter().map(|(h, _)| *h).collect();
            event!(
                "block_published",
                slot,
                n_blobs = block_blobs.len(),
                payload_blobs = n_payload,
                blobs = crate::events::hash_list(&block_hashes)
            );
        }

        // Drain events until t=4s
        drain_events_until(
            swarm,
            el,
            roles,
            &mut rng,
            slot_start + Duration::from_secs(4),
            metrics,
            &mut el_peer_count,
            &mut partial_state,
            &mut el_blob_pool,
        )
        .await;

        // ---------------------------------------------------------------
        // t=4s — Attestation deadline snapshot (§3 cell possession).
        //
        // At this point a partial-column node holds only the custody cells it
        // derived from its local EL blob pool (getBlobs); the builder seeds the
        // block's columns over CL *after* this deadline (t=4-6s). So `cells_held`
        // here is exactly "% of custody cells already possessed after the EL
        // fetch". Timing fields are placeholders (populated in later phases).
        // ---------------------------------------------------------------
        {
            let block_root = block_root_for_slot(slot);
            let (cells_held, n_blobs) = partial_state
                .assembler
                .custody_possession(&block_root, &partial_state.custody_columns);
            let num_custody_columns = if roles.is_builder() {
                NUM_CUSTODY_COLUMNS as usize
            } else {
                partial_state.custody_columns.len()
            };
            let cells_total = partial_state.custody_columns.len() * n_blobs;
            let cl_peers = swarm.connected_peers().count();
            event!(
                "readiness",
                slot,
                is_cl_node = true,
                is_builder = roles.is_builder(),
                is_zk_attester = roles.is_zk_attester(),
                eligible_envelope = !roles.is_builder() && !roles.is_zk_attester(),
                eligible_custody = partial_state.enabled && !roles.is_builder(),
                n_blobs = n_blobs,
                num_custody_columns = num_custody_columns,
                cells_held = cells_held,
                cells_total = cells_total,
                cl_peers = cl_peers,
                el_peers = el_peer_count,
                block_t_ms = crate::events::OptMs(None),
                envelope_t_ms = crate::events::OptMs(None),
                custody_complete_t_ms = crate::events::OptMs(None),
            );
        }

        // ---------------------------------------------------------------
        // t=4-6s — Payload & blob release phase (builder only)
        //
        // The builder reveals the execution payload envelope and seeds the
        // block's data columns / blob sidecars. (Validators that already held
        // the blobs may have started propagating columns earlier, off the t=0
        // proposal.)
        // ---------------------------------------------------------------
        if roles.is_builder() {
            // Publish the payload-reveal envelope. Blob commitments were already
            // announced in the t=0 proposal, so the envelope doesn't repeat them.
            // Under blocks-in-blobs the payload *also* rides the column subnets as
            // payload-blobs (below); the two paths coexist, and only zk-attesters
            // (unsubscribed from this topic) rely solely on the column path.
            let envelope =
                SignedExecutionPayloadEnvelope::new(slot, node_index, execution_payload.clone());
            publish_gossip(
                swarm,
                TOPIC_CL_PAYLOAD_ENVELOPE,
                &GossipMessage::Envelope(envelope),
                metrics,
            );
            info!(
                slot,
                blobs = block_blobs.len(),
                payload_blobs = n_payload,
                payload_bytes = execution_payload.len(),
                "builder: published payload envelope"
            );

            // Wrap the pooled EL blobs (and any payload-blobs) as this block's sidecars.
            let blobs: Vec<BlobSidecar> = block_blobs
                .iter()
                .enumerate()
                .map(|(i, (hash, data))| BlobSidecar {
                    blob_index: i as u64,
                    slot,
                    kzg_commitment: commitment_for_blob_hash(hash),
                    kzg_proof: vec![0xEE; KZG_ELEMENT_SIZE],
                    blob_data: data.clone(),
                })
                .collect();

            if blobs.is_empty() {
                // Nothing arrived over EL networking (e.g. no spammer peered).
                info!(slot, "builder: no pooled EL blobs to include in this block");
            } else if partial_state.enabled {
                // Partial path: reshape blobs into data column sidecars and seed
                // every column via the gossipsub 1.3 partial protocol. The builder
                // holds all of the block's blobs, so it can seed every column
                // fully. No full blob sidecars are published on this path.
                let block_root = block_root_for_slot(slot);
                let header = PartialDataColumnHeader::from_commitments(commitments);
                let columns = blobs_to_data_column_sidecars(&blobs, &header);
                for col in columns {
                    publish_column_partial(
                        swarm,
                        &mut partial_state,
                        &header,
                        block_root,
                        col,
                        metrics,
                    );
                }
                // The builder already holds everything; mark its custody columns
                // as advertised so the shared custody path doesn't re-publish.
                partial_state.custody_advertised_blocks.insert(block_root);
                info!(
                    slot,
                    blobs = blobs.len(),
                    columns = NUM_CUSTODY_COLUMNS,
                    "builder: seeded data columns via partial messages"
                );
                // Blob/column release time — §4 measures custody-fetch completion
                // from this point.
                event!(
                    "columns_seeded",
                    slot,
                    columns = NUM_CUSTODY_COLUMNS,
                    n_blobs = blobs.len()
                );
            } else {
                // Baseline path: publish full 128 KiB blob sidecars.
                let count = blobs.len();
                for sidecar in blobs {
                    publish_gossip(
                        swarm,
                        TOPIC_CL_BLOB_SIDECAR,
                        &GossipMessage::Sidecar(sidecar),
                        metrics,
                    );
                }
                info!(slot, blobs = count, "builder: published blob sidecars");
            }
        }

        // t=5..11s — once per second, re-advertise custody columns still
        // missing cells (for every tracked block, not just this slot's), so
        // nodes whose advertisement expired from peers' partial-message state
        // (~3.5s TTL) before the t=4s column seeding keep getting shots at the
        // deltas (see `readvertise_incomplete_custody_columns`). By t=5s the
        // t≈0 publish has aged out of our own behaviour cache everywhere, so
        // the first re-advertisement is guaranteed to actually transmit;
        // later passes give multi-hop stragglers more rounds. Once a column
        // completes the sweep is a no-op, and while our cached publish is
        // fresh the extension's stale-metadata check suppresses the resend, so
        // steady-state cost is nil.
        for readvertise_at in 5..=11 {
            drain_events_until(
                swarm,
                el,
                roles,
                &mut rng,
                slot_start + Duration::from_secs(readvertise_at),
                metrics,
                &mut el_peer_count,
                &mut partial_state,
                &mut el_blob_pool,
            )
            .await;
            readvertise_incomplete_custody_columns(swarm, &mut partial_state, metrics);
        }

        // Drain events until t=12s (slot boundary)
        drain_events_until(
            swarm,
            el,
            roles,
            &mut rng,
            slot_start + Duration::from_secs(12),
            metrics,
            &mut el_peer_count,
            &mut partial_state,
            &mut el_blob_pool,
        )
        .await;

        // Emit per-slot bandwidth summary
        metrics.emit_slot_summary(slot);

        let cl_peers = swarm.connected_peers().count();
        event!(
            "slot_end",
            slot,
            cl_peers = cl_peers,
            el_peers = el_peer_count
        );
        info!(slot, "=== SLOT END ===");
    }

    // Jobs whose deadlines extend past the configured simulation window are
    // explicitly right-censored rather than left as unmatched start events.
    partial_state.drop_pending_reconstructions("simulation-ended");

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
    seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(node_id)
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
    // Blob-spammers are EL-only: partial columns are disabled and they are not
    // builders, so the shared EL handler never pools blobs for them.
    let partial_state = PartialState::new(false, true, seed, CUSTODY_SUBSET_SIZE, None);
    let mut el_blob_pool = ElBlobPool::default();

    info!(%roles, num_slots, blobs_per_slot, node_id, "starting blob-spammer");

    for slot in 0..num_slots {
        info!(slot, %roles, "=== SLOT START ===");
        event!("slot_start", slot);
        metrics.set_slot(slot);
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
                    // Blob creation time (the origin timestamp §2 latencies measure from).
                    for h in &announce.blob_hashes {
                        event!("blob_offered", slot, blob = crate::events::hex_bytes(h));
                    }
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
                        // Return is always None (partials disabled for spammers).
                        let _ = handle_el_event(
                            el,
                            roles,
                            &mut rng,
                            ev,
                            metrics,
                            &mut el_peer_count,
                            &partial_state,
                            &mut el_blob_pool,
                        );
                    }
                }
            }
        }

        info!(
            slot,
            produced,
            peers = el_peer_count,
            "spammer: slot complete"
        );
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
    el_peer_count: &mut usize,
    partial_state: &mut PartialState,
    el_blob_pool: &mut ElBlobPool,
) {
    loop {
        let now = Instant::now();
        let due_by = if now < deadline { now } else { deadline };
        if process_due_reconstructions(swarm, partial_state, metrics, due_by) {
            partial_state.retry_eligible();
        }
        if now >= deadline {
            break;
        }
        let wake_at = partial_state
            .scheduled_reconstructions
            .first()
            .map(|batch| batch.ready_at.min(deadline))
            .unwrap_or(deadline);
        tokio::select! {
            _ = tokio::time::sleep_until(wake_at) => {}
            event = swarm.select_next_some() => {
                handle_swarm_event(swarm, roles, event, metrics, partial_state, el_blob_pool);
            }
            ev = el.event_rx.recv() => {
                if let Some(ev) = ev {
                    // A grown blob pool may let us derive more custody cells for
                    // any block whose header we already know (hash matching).
                    if handle_el_event(
                        el, roles, rng, ev, metrics, el_peer_count, partial_state, el_blob_pool,
                    ) {
                        for block_root in partial_state.assembler.blocks_with_header() {
                            ensure_custody_columns(swarm, partial_state, el_blob_pool, block_root, metrics);
                        }
                    }
                }
            }
        }
    }
}

/// Apply all jobs due at this wake, then publish each changed column once.
fn process_due_reconstructions(
    swarm: &mut Swarm<SimBehaviour>,
    partial_state: &mut PartialState,
    metrics: &mut BandwidthMetrics,
    due_by: Instant,
) -> bool {
    let due_count = partial_state
        .scheduled_reconstructions
        .iter()
        .take_while(|batch| batch.ready_at <= due_by)
        .count();
    if due_count == 0 {
        return false;
    }
    let due: Vec<_> = partial_state
        .scheduled_reconstructions
        .drain(..due_count)
        .collect();
    let mut changed_by_block: BTreeMap<[u8; 32], HashSet<u64>> = BTreeMap::new();
    let mut affected_blocks = BTreeSet::new();
    for batch in due {
        let slot = slot_for_block_root(&batch.block_root);
        match partial_state.assembler.apply_reconstruction(
            batch.block_root,
            batch.generation,
            &batch.rows,
        ) {
            ReconstructionApply::Stale => {
                for blob_index in batch.rows {
                    event!(
                        "blob_reconstruction_dropped",
                        slot,
                        blob_index = blob_index,
                        generation = batch.generation,
                        attempt = batch.attempt,
                        trigger = batch.trigger,
                        reason = "assembly-evicted"
                    );
                }
            }
            ReconstructionApply::Applied {
                rows,
                changed_columns,
                newly_complete_columns,
            } => {
                for _ in 0..newly_complete_columns {
                    metrics.record_partial_column_completed();
                }
                for row in rows {
                    event!(
                        "blob_reconstruction_completed",
                        slot,
                        blob_index = row.blob_index,
                        generation = batch.generation,
                        attempt = batch.attempt,
                        trigger = batch.trigger,
                        cells_added = row.cells_added,
                        columns_updated = row.columns_updated,
                        outcome = if row.already_complete {
                            "already-complete"
                        } else {
                            "reconstructed"
                        }
                    );
                }
                changed_by_block
                    .entry(batch.block_root)
                    .or_default()
                    .extend(changed_columns);
                affected_blocks.insert(batch.block_root);
            }
        }
    }
    for (block_root, columns) in changed_by_block {
        let mut columns: Vec<_> = columns.into_iter().collect();
        columns.sort_unstable();
        for column in columns {
            republish_partial_column(swarm, partial_state, block_root, column, metrics);
        }
    }
    for block_root in affected_blocks {
        maybe_emit_custody_complete(partial_state, block_root);
    }
    true
}

// ---------------------------------------------------------------------------
// CL swarm event handling
// ---------------------------------------------------------------------------

/// Handle a single CL swarm event.
#[allow(clippy::too_many_arguments)]
fn handle_swarm_event(
    swarm: &mut Swarm<SimBehaviour>,
    roles: &NodeRoles,
    event: SwarmEvent<SimBehaviourEvent>,
    metrics: &mut BandwidthMetrics,
    partial_state: &mut PartialState,
    el_blob_pool: &mut ElBlobPool,
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
            if let Ok(msg) = bincode::deserialize::<GossipMessage>(&message.data) {
                // On seeing the t=0 proposal, a non-builder validator (a) if
                // partials are enabled, records the block header and derives/
                // advertises its custody columns from any of the block's blobs it
                // already holds (local `engine_getBlobs` — a local read, no fetch),
                // then (b) evicts those now-included blobs from its EL pool so they
                // are not re-pooled or re-included later (mempool eviction). Step
                // (a) must run before (b) so getBlobs can still read the blobs.
                if !roles.is_builder() {
                    if let GossipMessage::BeaconBlock(block) = &msg {
                        // Blobless blocks have no columns to fetch or advertise.
                        if !block.blob_kzg_commitments.is_empty() {
                            if partial_state.enabled {
                                let block_root = block_root_for_slot(block.slot);
                                // Commitments embed the blobs' hashes so the local
                                // getBlobs analog can match the pool by hash.
                                let header = PartialDataColumnHeader::from_commitments(
                                    block.blob_kzg_commitments.clone(),
                                );
                                partial_state.assembler.set_header(block_root, header);
                                ensure_custody_columns(
                                    swarm,
                                    partial_state,
                                    el_blob_pool,
                                    block_root,
                                    metrics,
                                );
                            }
                            for commitment in &block.blob_kzg_commitments {
                                if let Some(hash) = blob_hash_from_commitment(commitment) {
                                    el_blob_pool.mark_included(hash, block.slot);
                                }
                            }
                        }
                    }
                }
                handle_gossip_message(msg);
            } else {
                warn!(%topic, "failed to deserialize gossip message");
            }
        }

        // -- Partial data-column message received (gossipsub 1.3) --
        SwarmEvent::Behaviour(SimBehaviourEvent::Gossipsub(gossipsub::Event::Partial {
            topic_hash,
            peer_id,
            group_id,
            message,
            metadata,
        })) => {
            let meta_bytes = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            let subnet = subnet_from_topic(topic_hash.as_str());
            match (subnet, message) {
                (Some(subnet), Some(msg)) => {
                    let bytes = msg.len() + meta_bytes;
                    match decode_partial(subnet, &group_id, &msg) {
                        Ok(column) => {
                            let has_header = column.sidecar.header.is_some();
                            let result = partial_state.assembler.merge_partial(&column);
                            metrics.record_partial_received(bytes, result.added_cells, has_header);
                            debug!(
                                subnet,
                                index = column.index,
                                added_cells = result.added_cells,
                                has_header,
                                "partial data column received"
                            );
                            if result.newly_complete.is_some() {
                                metrics.record_partial_column_completed();
                                debug!(
                                    subnet,
                                    index = column.index,
                                    "data column completed via partials"
                                );
                            }
                            // Once we learn the block header (e.g. before the
                            // envelope arrived), derive/advertise our custody
                            // columns so peers send us the cells we lack.
                            if has_header {
                                ensure_custody_columns(
                                    swarm,
                                    partial_state,
                                    el_blob_pool,
                                    column.block_root,
                                    metrics,
                                );
                            }
                            // Always check the just-merged partial itself. The
                            // custody helper can return early when there is no EL
                            // data to fold in, including on a header refresh.
                            partial_state.schedule_eligible(column.block_root);
                            // Re-publish our accumulated cells so we can serve them
                            // to other mesh peers (multi-hop cell-delta cross-fill).
                            // Also re-publish when the sender's metadata shows state
                            // to repair (it holds cells we lack, or wants cells we
                            // hold): the partial extension forgets all local/per-peer
                            // state after ~3.5s (5 heartbeats), so by the time the
                            // builder seeds columns at t=4s our t≈0 custody
                            // advertisement has expired everywhere and must be
                            // rebuilt on demand.
                            if result.added_cells > 0
                                || partial_repair_needed(
                                    partial_state,
                                    &column.block_root,
                                    column.index,
                                    metadata.as_deref(),
                                )
                            {
                                republish_partial_column(
                                    swarm,
                                    partial_state,
                                    column.block_root,
                                    column.index,
                                    metrics,
                                );
                            }
                            // May have just completed our full custody set (§4).
                            maybe_emit_custody_complete(partial_state, column.block_root);
                        }
                        Err(e) => {
                            debug!(error = %e, "failed to decode partial column; reporting invalid");
                            swarm
                                .behaviour_mut()
                                .gossipsub
                                .report_invalid_partial(peer_id, &topic_hash);
                        }
                    }
                }
                // Metadata-only exchange (no payload): account the metadata bytes.
                (Some(subnet), None) => {
                    if meta_bytes > 0 {
                        metrics.record_partial_received(meta_bytes, 0, false);
                    }
                    // A metadata-only message is the extension's "poke" after its
                    // ~3.5s state TTL wiped a peer's view of us (e.g. the builder's
                    // post-seed advertisement on subnets where the block header was
                    // already delivered). If it reveals a mismatch, re-publish to
                    // re-advertise our requests / serve the peer's.
                    if let Some(block_root) = block_root_from_group_id(&group_id) {
                        if partial_repair_needed(
                            partial_state,
                            &block_root,
                            subnet,
                            metadata.as_deref(),
                        ) {
                            republish_partial_column(
                                swarm,
                                partial_state,
                                block_root,
                                subnet,
                                metrics,
                            );
                        }
                    }
                }
                _ => {}
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

/// Handle a decoded CL gossip message (logging only; propagation side effects
/// are driven in `handle_swarm_event`).
fn handle_gossip_message(msg: GossipMessage) {
    match msg {
        GossipMessage::BeaconBlock(block) => {
            // Per-node beacon-block arrival (gossipsub delivers once per node), for
            // §3's block-propagation CDF.
            event!(
                "arrival",
                block.slot,
                atype = "block",
                n_blobs = block.blob_kzg_commitments.len()
            );
            info!(
                slot = block.slot,
                proposer = block.proposer_index,
                commitments = block.blob_kzg_commitments.len(),
                "received beacon block proposal"
            );
        }

        GossipMessage::Envelope(env) => {
            info!(
                slot = env.slot,
                builder = env.builder_index,
                payload_bytes = env.payload.len(),
                "received payload envelope"
            );
        }

        GossipMessage::Sidecar(sidecar) => {
            info!(
                slot = sidecar.slot,
                blob_index = sidecar.blob_index,
                "received blob sidecar"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// EL/TCP event handling
// ---------------------------------------------------------------------------

/// Handle a single EL/TCP event.
///
/// Full blobs this node's EL receives (via the normal EL announce → pull flow)
/// are added to the local EL blob pool, keyed by announced hash: builders
/// include them in their next block, and partial-column nodes read them via the
/// local `engine_getBlobs` analog. Returns `true` when the pool grew and a
/// partial-column node should re-check its known blocks for newly derivable
/// cells. Blob-spammers pass a disabled `partial_state` and never pool.
#[allow(clippy::too_many_arguments)]
fn handle_el_event(
    el: &ElHandle,
    roles: &NodeRoles,
    rng: &mut StdRng,
    event: ElEvent,
    metrics: &mut BandwidthMetrics,
    el_peer_count: &mut usize,
    partial_state: &PartialState,
    el_blob_pool: &mut ElBlobPool,
) -> bool {
    match event {
        ElEvent::PeerConnected(peer) => {
            *el_peer_count += 1;
            info!(peer, peers = *el_peer_count, "EL: peer connected");
            false
        }
        ElEvent::PeerDisconnected(peer) => {
            *el_peer_count = el_peer_count.saturating_sub(1);
            info!(peer, peers = *el_peer_count, "EL: peer disconnected");
            false
        }
        ElEvent::Message { from, msg, bytes } => {
            if let ElMessage::FullPayloadResponse(resp) = &msg {
                return handle_full_payload(
                    roles,
                    from,
                    resp,
                    bytes,
                    metrics,
                    partial_state,
                    el_blob_pool,
                );
            }
            if let ElMessage::CustodyResponse(resp) = &msg {
                return handle_custody_response(
                    from,
                    resp,
                    bytes,
                    metrics,
                    partial_state,
                    el_blob_pool,
                );
            }
            serve_el_message(
                el,
                roles,
                &partial_state.custody_columns,
                rng,
                from,
                msg,
                bytes,
                metrics,
            );
            false
        }
    }
}

/// Handle a received full payload: pool it (for nodes that pool blobs) and
/// account the bandwidth. Returns whether the blob pool grew.
fn handle_full_payload(
    roles: &NodeRoles,
    from: ElPeerId,
    resp: &FullPayloadResponse,
    bytes: usize,
    metrics: &mut BandwidthMetrics,
    partial_state: &PartialState,
    el_blob_pool: &mut ElBlobPool,
) -> bool {
    metrics.record_response_received(bytes);
    if resp.blob_hash.len() != 32 {
        warn!(from, "EL: full payload with malformed blob hash; dropped");
        return false;
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&resp.blob_hash);
    event!(
        "arrival",
        resp.slot,
        atype = "full_payload",
        blob = crate::events::hex_bytes(&hash)
    );

    let pools_blobs =
        roles.is_builder() || (partial_state.enabled && partial_state.get_blobs_enabled);
    info!(
        slot = resp.slot,
        payload_size = resp.payload_data.len(),
        from,
        pooled = pools_blobs,
        "EL: received full payload"
    );
    if pools_blobs {
        el_blob_pool.insert_full(hash, resp.payload_data.to_vec());
        // Newly pooled blobs may let a partial-column node derive more custody
        // cells right away. (Builders consume the pool at slot start instead.)
        return !roles.is_builder() && partial_state.enabled && partial_state.get_blobs_enabled;
    }
    false
}

/// Handle received custody cells: pool them as partial blob data (for nodes on
/// the partial-column getBlobs path) and account the bandwidth. Returns whether
/// the pool grew — so the caller can re-derive custody columns for known blocks.
///
/// Mirrors [`handle_full_payload`], but stores sparse cells rather than a full
/// blob. Builders never sample (they pull full payloads) and can't block-include
/// partial data, so the builder branch of `pools_blobs` does not apply here.
fn handle_custody_response(
    from: ElPeerId,
    resp: &CustodyCellResponse,
    bytes: usize,
    metrics: &mut BandwidthMetrics,
    partial_state: &PartialState,
    el_blob_pool: &mut ElBlobPool,
) -> bool {
    metrics.record_response_received(bytes);
    if resp.blob_hash.len() != 32 {
        warn!(from, "EL: custody cells with malformed blob hash; dropped");
        return false;
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&resp.blob_hash);
    event!(
        "arrival",
        resp.slot,
        atype = "custody_cells",
        blob = crate::events::hex_bytes(&hash),
        cells = resp.cells.len()
    );

    // Pool cells iff the getBlobs read path can consume them: when
    // `get_blobs_enabled` is false, `ensure_custody_columns` skips the pool
    // entirely, so pooled cells would be dead weight.
    let pools_cells = partial_state.enabled && partial_state.get_blobs_enabled;
    info!(
        slot = resp.slot,
        cells = resp.cells.len(),
        from,
        pooled = pools_cells,
        "EL: received custody cells"
    );
    if !pools_cells {
        return false;
    }
    let added =
        el_blob_pool.insert_cells(hash, resp.cells.iter().map(|c| (c.column, c.data.to_vec())));
    if added > 0 {
        metrics.record_partial_cells_pooled(added);
    }
    added > 0
}

/// Dispatch an inbound EL message based on type and this node's roles. Handles
/// serving (announce reactions, request → response) without touching the CL.
#[allow(clippy::too_many_arguments)]
fn serve_el_message(
    el: &ElHandle,
    roles: &NodeRoles,
    custody_columns: &[u64],
    rng: &mut StdRng,
    from: ElPeerId,
    msg: ElMessage,
    bytes: usize,
    metrics: &mut BandwidthMetrics,
) {
    match msg {
        // -- Blob hash announcement: peers pull what they need per blob --
        ElMessage::Announce(announce) => {
            metrics.record_el_announce_received(bytes);
            info!(
                slot = announce.slot,
                hashes = announce.blob_hashes.len(),
                from,
                "EL: received blob hash announce"
            );
            // Builders always behave as providers: they must hold the full blob
            // data to include the blobs in the next block they build.
            if roles.is_builder() {
                send_full_payload_requests(el, from, &announce, metrics);
            } else if !roles.is_blob_spammer() {
                send_random_sparse_blobpool_requests(
                    el,
                    custody_columns,
                    rng,
                    from,
                    &announce,
                    metrics,
                );
            }
        }

        // -- Incoming requests: the holder (builder) serves a response --
        ElMessage::CustodyRequest(req) => {
            metrics.record_request_received(bytes);
            info!(slot = req.slot, columns = ?req.column_indices, from, "EL: handling custody cell request");
            // Serve deterministic cells derived from the hash-derived blob (as the
            // full-payload server does): every holder serves identical bytes for
            // the same (blob, column), so persisted/propagated custody cells stay
            // byte-consistent network-wide and with full-blob-derived cells.
            let blob = payload_for_blob_hash(&req.blob_hash);
            let cells: Vec<CustodyCell> = req
                .column_indices
                .iter()
                .map(|&column| CustodyCell {
                    column,
                    data: Bytes::from(derive_cell(&blob, column)), // 2 KiB per cell
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
            // Serve the hash-derived payload: every holder serves identical
            // bytes for the same blob, deterministically.
            let response = ElMessage::FullPayloadResponse(FullPayloadResponse {
                slot: req.slot,
                blob_hash: req.blob_hash.clone(),
                payload_data: Bytes::from(payload_for_blob_hash(&req.blob_hash)),
            });
            let resp_bytes = response.encode().len() + 4;
            metrics.record_response_sent(resp_bytes);
            el.send(from, response);
        }

        // Custody responses are intercepted in `handle_el_event` before this
        // dispatch (see `handle_custody_response`); this arm is unreachable.
        ElMessage::CustodyResponse(resp) => {
            debug!(
                slot = resp.slot,
                from, "EL: unexpected custody response in serve path"
            );
        }
        // Full-payload responses are intercepted in `handle_el_event` before this
        // dispatch (see `handle_full_payload`); this arm is unreachable.
        ElMessage::FullPayloadResponse(resp) => {
            debug!(
                slot = resp.slot,
                from, "EL: unexpected full payload in serve path"
            );
        }
    }
}

/// Non-builder CL node: independently choose sampler/provider behavior for each
/// announced blob hash. Sparse blobpool (EIP-8070) has most blobs fetched as
/// custody cells, while a smaller fraction are fetched as full payloads.
fn send_random_sparse_blobpool_requests(
    el: &ElHandle,
    custody_columns: &[u64],
    rng: &mut StdRng,
    peer: ElPeerId,
    announce: &BlobHashAnnounce,
    metrics: &mut BandwidthMetrics,
) {
    for blob_hash in &announce.blob_hashes {
        if rng.gen_bool(SAMPLER_FETCH_PROBABILITY) {
            send_custody_request(
                el,
                custody_columns,
                rng,
                peer,
                announce.slot,
                blob_hash,
                metrics,
            );
        } else {
            send_full_payload_request(el, peer, announce.slot, blob_hash, metrics);
        }
    }
}

/// Sampler behavior: request this node's custody columns (+1 random extra) from
/// the announcing peer for one announced blob hash.
fn send_custody_request(
    el: &ElHandle,
    custody_columns: &[u64],
    rng: &mut StdRng,
    peer: ElPeerId,
    slot: u64,
    blob_hash: &Bytes,
    metrics: &mut BandwidthMetrics,
) {
    // The node's stable custody set + 1 random extra.
    let mut columns: HashSet<u64> = custody_columns.iter().copied().collect();
    columns.insert(rng.gen_range(0..NUM_CUSTODY_COLUMNS));
    let column_indices: Vec<u64> = columns.into_iter().collect();

    let request = ElMessage::CustodyRequest(CustodyCellRequest {
        slot,
        blob_hash: blob_hash.clone(),
        column_indices: column_indices.clone(),
    });
    let req_bytes = request.encode().len() + 4;
    info!(slot, columns = ?column_indices, peer, req_bytes, "sampler: sending custody cell request");
    metrics.record_request_sent(req_bytes);
    el.send(peer, request);
}

/// Provider behavior: request full payloads from the announcing peer.
fn send_full_payload_requests(
    el: &ElHandle,
    peer: ElPeerId,
    announce: &BlobHashAnnounce,
    metrics: &mut BandwidthMetrics,
) {
    for blob_hash in &announce.blob_hashes {
        send_full_payload_request(el, peer, announce.slot, blob_hash, metrics);
    }
}

/// Provider behavior: request the full payload for one announced blob hash.
fn send_full_payload_request(
    el: &ElHandle,
    peer: ElPeerId,
    slot: u64,
    blob_hash: &Bytes,
    metrics: &mut BandwidthMetrics,
) {
    let request = ElMessage::FullPayloadRequest(FullPayloadRequest {
        slot,
        blob_hash: blob_hash.clone(),
    });
    let req_bytes = request.encode().len() + 4;
    info!(
        slot,
        peer, req_bytes, "provider: sending full payload request"
    );
    metrics.record_request_sent(req_bytes);
    el.send(peer, request);
}

// ---------------------------------------------------------------------------
// Gossip publish helper
// ---------------------------------------------------------------------------

/// Serialize a `GossipMessage` with the compact binary codec and publish it on
/// the given topic. JSON would encode the large byte payloads (execution
/// payload, blob data) as number arrays — ~3.7 bytes on the wire per payload
/// byte — nearly quadrupling the modeled cost of envelopes and sidecars and
/// starving the builder's uplink right when it seeds columns.
fn publish_gossip(
    swarm: &mut Swarm<SimBehaviour>,
    topic_str: &str,
    msg: &GossipMessage,
    metrics: &mut BandwidthMetrics,
) {
    let topic = IdentTopic::new(topic_str);
    let data = bincode::serialize(msg).expect("serialize gossip message");

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

// ---------------------------------------------------------------------------
// Partial data-column helpers (gossipsub 1.3 cell-level deltas)
// ---------------------------------------------------------------------------

/// Ensure this node's custody columns for a block are registered and published
/// via the partial protocol, folding in any full blobs the local EL already
/// holds.
///
/// The pool read is our `engine_getBlobs` analog: a *local* Engine API call to
/// the node's own EL (whose pool was filled earlier over EL networking), never a
/// network request — so it adds no CL/EL traffic. Idempotent and incremental:
/// safe to call at envelope arrival, at header arrival, and again whenever the
/// pool grows; each call merges newly-derivable cells and republishes columns
/// that gained cells. Cells still missing are advertised as requests, so peers
/// deliver them over CL as cell-level deltas.
fn ensure_custody_columns(
    swarm: &mut Swarm<SimBehaviour>,
    partial_state: &mut PartialState,
    el_blob_pool: &ElBlobPool,
    block_root: [u8; 32],
    metrics: &mut BandwidthMetrics,
) {
    if !partial_state.enabled {
        return;
    }
    let Some(header) = partial_state.assembler.get_header(&block_root) else {
        return;
    };
    let num_blobs = header.kzg_commitments.len();
    let first_time = partial_state.custody_advertised_blocks.insert(block_root);

    // Local getBlobsV4: the block's commitments embed the announced hashes of its
    // blobs — match each against what our EL has pooled. Blob position i in the
    // block maps to bitmap bit i. For each (blob, custody column) the pool answers
    // with the cell if it holds it (full blob → derived; partial entry → the
    // sampled cell) or `None` if it lacks it, which we then advertise as a request.
    let slot = slot_for_block_root(&block_root);
    let hashes: Vec<Option<[u8; 32]>> = if partial_state.get_blobs_enabled {
        header
            .kzg_commitments
            .iter()
            .map(|c| blob_hash_from_commitment(c))
            .collect()
    } else {
        vec![None; num_blobs]
    };
    let have_any = hashes.iter().flatten().any(|h| el_blob_pool.contains(h));
    if !first_time && !have_any {
        return;
    }

    let custody = partial_state.custody_columns.clone();
    let mut pool_cells_added = 0usize;
    for &index in &custody {
        // Partial column from the pooled data: bit i set iff the EL can serve the
        // cell for (blob i, this custody column).
        let mut bitmap = CellBitmap::with_len(num_blobs);
        let mut column = Vec::new();
        let mut kzg_proofs = Vec::new();
        for (i, h) in hashes.iter().enumerate() {
            let Some(h) = h else { continue };
            let Some(cell) = el_blob_pool.get_cells(h, &[index]).pop().flatten() else {
                continue;
            };
            bitmap.set(i);
            column.push(cell);
            kzg_proofs.push(vec![0xEE; KZG_ELEMENT_SIZE]);
        }
        let partial = Arc::new(PartialDataColumn {
            block_root,
            index,
            sidecar: PartialDataColumnSidecar {
                cells_present_bitmap: bitmap,
                column,
                kzg_proofs,
                header: None,
            },
        });
        let result = partial_state.assembler.merge_partial(&partial);
        pool_cells_added += result.added_cells;
        if first_time || result.added_cells > 0 {
            if first_time && partial.sidecar.num_present() > 0 {
                metrics.record_partial_column_published();
            }
            republish_partial_column(swarm, partial_state, block_root, index, metrics);
        }
    }
    if pool_cells_added > 0 {
        info!(
            slot,
            cells = pool_cells_added,
            "getBlobs: derived custody cells from local EL blob pool"
        );
    } else if first_time {
        debug!(
            slot,
            custody = custody.len(),
            "advertised custody column requests over CL"
        );
    }

    // getBlobs may have just completed our full custody set before any CL seeding (§4).
    partial_state.schedule_eligible(block_root);
    maybe_emit_custody_complete(partial_state, block_root);
}

/// Publish one full data column via the gossipsub 1.3 partial protocol, recording
/// locally that we now hold its cells.
fn publish_column_partial(
    swarm: &mut Swarm<SimBehaviour>,
    partial_state: &mut PartialState,
    header: &PartialDataColumnHeader,
    block_root: [u8; 32],
    col: DataColumnSidecar,
    metrics: &mut BandwidthMetrics,
) {
    let index = col.index;
    let num_blobs = col.column.len();
    let sidecar = PartialDataColumnSidecar {
        cells_present_bitmap: CellBitmap::all_set(num_blobs),
        column: col.column,
        kzg_proofs: col.kzg_proofs,
        header: None,
    };
    let partial_column = Arc::new(PartialDataColumn {
        block_root,
        index,
        sidecar,
    });

    // Track locally that we hold this column's cells.
    partial_state
        .assembler
        .set_header(block_root, header.clone());
    partial_state.assembler.merge_partial(&partial_column);

    let header_sent = partial_state.header_tracker.get_for_block(block_root);
    let request_cells = CellBitmap::all_set(num_blobs);
    let outgoing = OutgoingPartialColumn::new(
        Arc::clone(&partial_column),
        header,
        header_sent,
        request_cells,
    );

    let topic = data_column_topic(subnet_for_column(index));
    match swarm
        .behaviour_mut()
        .gossipsub
        .publish_partial(topic.hash(), outgoing)
    {
        Ok(bytes) => {
            metrics.record_partial_column_published();
            metrics.record_partial_sent(bytes);
        }
        Err(e) => debug!(index, error = %e, "publish_partial failed"),
    }
}

/// Re-publish our current (possibly incomplete) partial for a column so the
/// gossipsub behaviour will serve our accumulated cells to peers. This is what
/// makes cell-level deltas cross-fill across multiple hops.
fn republish_partial_column(
    swarm: &mut Swarm<SimBehaviour>,
    partial_state: &mut PartialState,
    block_root: [u8; 32],
    index: u64,
    metrics: &mut BandwidthMetrics,
) {
    let Some(header) = partial_state.assembler.get_header(&block_root) else {
        return;
    };
    let Some(partial) = partial_state.assembler.current_partial(&block_root, index) else {
        return;
    };
    let num_blobs = partial.sidecar.cells_present_bitmap.len();
    let header_sent = partial_state.header_tracker.get_for_block(block_root);
    // We still want every cell we don't yet hold.
    let request_cells = CellBitmap::all_set(num_blobs);
    let outgoing =
        OutgoingPartialColumn::new(Arc::new(partial), &header, header_sent, request_cells);
    let topic = data_column_topic(subnet_for_column(index));
    // Re-publish serves our accumulated cells (and re-advertises requests) to peers;
    // account the payload it queues as outbound CL bandwidth, but without counting a
    // new published column (this column was already counted at first publish).
    if let Ok(bytes) = swarm
        .behaviour_mut()
        .gossipsub
        .publish_partial(topic.hash(), outgoing)
    {
        metrics.record_partial_sent(bytes);
    }
}

/// Parse the block root out of a partial group id (`0x00 || block_root`).
fn block_root_from_group_id(group_id: &[u8]) -> Option<[u8; 32]> {
    if group_id.first() != Some(&PARTIAL_COLUMNS_VERSION_BYTE) || group_id.len() != 33 {
        return None;
    }
    let mut block_root = [0u8; 32];
    block_root.copy_from_slice(&group_id[1..33]);
    Some(block_root)
}

/// Decode a gossipsub `Event::Partial` payload into a [`PartialDataColumn`]. The
/// column index comes from the topic's subnet (1:1 under Fulu); the block root
/// comes from the group id (`0x00 || block_root`).
fn decode_partial(subnet: u64, group_id: &[u8], data: &[u8]) -> Result<PartialDataColumn, String> {
    let block_root = block_root_from_group_id(group_id).ok_or_else(|| {
        format!(
            "bad group id (len {}): {:?}",
            group_id.len(),
            group_id.first()
        )
    })?;
    let sidecar =
        PartialDataColumnSidecar::decode(data).map_err(|e| format!("decode sidecar: {e}"))?;
    if !sidecar.is_structurally_valid() {
        return Err("invalid sidecar bitmap/cell/proof structure".into());
    }
    Ok(PartialDataColumn {
        block_root,
        index: subnet,
        sidecar,
    })
}

/// Whether a peer's advertised metadata calls for re-publishing our partial for
/// `(block_root, index)`: the peer either holds cells we still lack (so
/// re-advertising our `requests` prompts it to send them), or wants cells we
/// hold (so re-publishing re-arms the behaviour's expired local cache and it
/// serves the delta from our stored metadata of that peer).
///
/// This on-demand repair exists because the gossipsub partial extension expires
/// every cached local partial and per-peer metadata after 5 heartbeats (~3.5s)
/// without refresh, which is shorter than the proposal→column-seeding gap
/// (t≈0 → t=4s). Re-publishing when both sides are already in sync is skipped
/// by the extension's own stale-metadata check, so this stays quiet once a
/// column has converged.
fn partial_repair_needed(
    partial_state: &PartialState,
    block_root: &[u8; 32],
    index: u64,
    metadata: Option<&[u8]>,
) -> bool {
    if !partial_state.enabled {
        return false;
    }
    let Some(bytes) = metadata else {
        return false;
    };
    if bytes.is_empty() {
        return false;
    }
    let Ok(peer_meta) = PartialDataColumnPartsMetadata::decode(bytes) else {
        return false;
    };
    let ours = match partial_state.assembler.current_partial(block_root, index) {
        Some(partial) => partial.sidecar.cells_present_bitmap,
        None => CellBitmap::with_len(peer_meta.available.len()),
    };
    // Peer has cells we lack → re-advertise our requests so it sends them.
    if !peer_meta.available.is_subset(&ours) {
        return true;
    }
    // Peer wants cells we hold → re-publish so the behaviour serves the delta.
    peer_meta
        .requests
        .difference(&peer_meta.available)
        .intersects(&ours)
}

/// Re-advertise our custody columns that are still missing cells — for every
/// tracked block with a known header — re-registering our `requests` bitmap
/// with peers.
///
/// Safety net for the partial extension's ~3.5s state TTL: the custody
/// advertisement made at proposal arrival (t≈0) has expired network-wide by the
/// time the builder seeds columns at t=4s, and the reactive repair path
/// ([`partial_repair_needed`]) only fires on an incoming poke — which a node
/// outside the builder's mesh may not get, and which is silently dropped by the
/// extension's stale-metadata check while our previous (expired-remotely but
/// still cached locally) publish lingers. Calling this mid-slot, well past the
/// TTL of the t≈0 publish, guarantees a fresh advertisement actually goes out;
/// peers holding the missing cells respond with deltas. Covering all tracked
/// blocks lets columns that spilled past their slot boundary finish instead of
/// staying censored.
fn readvertise_incomplete_custody_columns(
    swarm: &mut Swarm<SimBehaviour>,
    partial_state: &mut PartialState,
    metrics: &mut BandwidthMetrics,
) {
    if !partial_state.enabled {
        return;
    }
    let custody = partial_state.custody_columns.clone();
    // Newest two blocks only (current + previous slot): that is where deltas
    // can still realistically arrive, and sweeping older blocks just churns
    // peers with advertisements nobody can serve anymore.
    let recent_blocks: Vec<[u8; 32]> = partial_state
        .assembler
        .blocks_with_header()
        .into_iter()
        .rev()
        .take(2)
        .collect();
    for block_root in recent_blocks {
        for &index in &custody {
            let incomplete = partial_state
                .assembler
                .current_partial(&block_root, index)
                .map(|partial| !partial.sidecar.is_complete())
                .unwrap_or(true);
            if incomplete {
                republish_partial_column(swarm, partial_state, block_root, index, metrics);
            }
        }
    }
}

// Re-export the generated event type for the combined behaviour.
use crate::network::SimBehaviourEvent;

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u8) -> [u8; 32] {
        let mut x = [0u8; 32];
        x[0] = n;
        x
    }

    /// A 2 KiB cell filled with byte `b`, paired with its column index.
    fn cell(col: u64, b: u8) -> (u64, Vec<u8>) {
        (col, vec![b; BYTES_PER_CELL])
    }

    /// The FIFO order of pooled hashes' first bytes, oldest first.
    fn order(pool: &ElBlobPool) -> Vec<u8> {
        pool.pending.iter().map(|(hh, _)| hh[0]).collect()
    }

    #[test]
    fn take_pending_caps_carries_forward_and_never_reincludes() {
        let mut pool = ElBlobPool::default();
        for i in 0..9 {
            pool.insert_full(h(i), vec![i]);
        }
        // Slot 0 includes the cap (6); the other 3 stay pending.
        let s0 = pool.take_pending(MAX_BLOBS_PER_BLOCK, 0);
        assert_eq!(s0.len(), 6);
        // Slot 1 gets the remaining 3 — none of slot 0's blobs come back.
        let s1 = pool.take_pending(MAX_BLOBS_PER_BLOCK, 1);
        assert_eq!(s1.len(), 3);
        // Slot 2 has nothing left; no blob is ever included twice.
        assert!(pool.take_pending(MAX_BLOBS_PER_BLOCK, 2).is_empty());
        let mut all: Vec<[u8; 32]> = s0.iter().chain(&s1).map(|(hash, _)| *hash).collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 9, "all included hashes are distinct");
    }

    #[test]
    fn included_blob_is_refused_re_entry() {
        let mut pool = ElBlobPool::default();
        pool.insert_full(h(1), vec![1]);
        assert_eq!(pool.take_pending(MAX_BLOBS_PER_BLOCK, 0).len(), 1);
        // Re-announced after inclusion → not re-pooled, not re-included.
        pool.insert_full(h(1), vec![1]);
        assert!(!pool.contains(&h(1)));
        assert!(pool.take_pending(MAX_BLOBS_PER_BLOCK, 1).is_empty());
    }

    #[test]
    fn mark_included_evicts_from_pending() {
        let mut pool = ElBlobPool::default();
        pool.insert_full(h(2), vec![2]);
        assert!(pool.contains(&h(2)));
        pool.mark_included(h(2), 0);
        assert!(!pool.contains(&h(2)));
        // And it won't be re-poolable within the window.
        pool.insert_full(h(2), vec![2]);
        assert!(!pool.contains(&h(2)));
    }

    #[test]
    fn prune_forgets_inclusion_after_window() {
        let mut pool = ElBlobPool::default();
        pool.mark_included(h(3), 0);
        // Still within the 4-slot window → remembered.
        pool.prune_included(INCLUDED_WINDOW_SLOTS - 1);
        pool.insert_full(h(3), vec![3]);
        assert!(!pool.contains(&h(3)));
        // Window elapsed → forgotten, re-poolable.
        pool.prune_included(INCLUDED_WINDOW_SLOTS);
        pool.insert_full(h(3), vec![3]);
        assert!(pool.contains(&h(3)));
    }

    // -- Partial-column repair (cell-delta re-advertisement) --

    fn bitmap(len: usize, set: &[usize]) -> CellBitmap {
        let mut b = CellBitmap::with_len(len);
        for &i in set {
            b.set(i);
        }
        b
    }

    fn meta(len: usize, available: &[usize], requests: &[usize]) -> Vec<u8> {
        PartialDataColumnPartsMetadata {
            available: bitmap(len, available),
            requests: bitmap(len, requests),
        }
        .encode()
    }

    /// A partial-enabled state holding cells `held` (of `len` blob slots) for
    /// (block `h(1)`, column 0).
    fn state_with_cells(len: usize, held: &[usize]) -> PartialState {
        let mut state = PartialState::new(true, true, 7, CUSTODY_SUBSET_SIZE, None);
        let sidecar = PartialDataColumnSidecar {
            cells_present_bitmap: bitmap(len, held),
            column: held.iter().map(|_| vec![0xAA; BYTES_PER_CELL]).collect(),
            kzg_proofs: held.iter().map(|_| vec![0xEE; KZG_ELEMENT_SIZE]).collect(),
            header: None,
        };
        state.assembler.merge_partial(&PartialDataColumn {
            block_root: h(1),
            index: 0,
            sidecar,
        });
        state
    }

    #[test]
    fn repair_needed_when_peer_has_cells_we_lack() {
        // We hold the 4 EL cells (2..6); the peer advertises the payload cells
        // (0, 1) — the post-seed builder case. Must re-advertise our requests.
        let state = state_with_cells(6, &[2, 3, 4, 5]);
        let peer = meta(6, &[0, 1, 2, 3, 4, 5], &[0, 1, 2, 3, 4, 5]);
        assert!(partial_repair_needed(&state, &h(1), 0, Some(&peer)));
    }

    #[test]
    fn repair_needed_when_peer_wants_cells_we_hold() {
        // The peer lacks cells 2..6 (wants everything) and we hold them: we must
        // re-publish so the (possibly expired) behaviour cache serves the delta.
        let state = state_with_cells(6, &[2, 3, 4, 5]);
        let peer = meta(6, &[0, 1], &[0, 1, 2, 3, 4, 5]);
        assert!(partial_repair_needed(&state, &h(1), 0, Some(&peer)));
    }

    #[test]
    fn repair_not_needed_when_nothing_to_trade() {
        // The peer only wants cells neither of us has, and holds a subset of
        // ours — no message could make progress.
        let state = state_with_cells(6, &[2, 3, 4, 5]);
        let peer = meta(6, &[2, 3, 4, 5], &[0, 1, 2, 3, 4, 5]);
        assert!(!partial_repair_needed(&state, &h(1), 0, Some(&peer)));
    }

    #[test]
    fn repair_not_needed_when_both_sides_complete_or_meta_missing() {
        let state = state_with_cells(6, &[0, 1, 2, 3, 4, 5]);
        let peer = meta(6, &[0, 1, 2, 3, 4, 5], &[0, 1, 2, 3, 4, 5]);
        assert!(!partial_repair_needed(&state, &h(1), 0, Some(&peer)));
        assert!(!partial_repair_needed(&state, &h(1), 0, None));
        assert!(!partial_repair_needed(&state, &h(1), 0, Some(&[])));
    }

    #[test]
    fn repair_needed_for_unknown_column_when_peer_has_cells() {
        // No local cells at all for the block: any advertised cell is one we lack.
        let state = PartialState::new(true, true, 7, CUSTODY_SUBSET_SIZE, None);
        let peer = meta(6, &[0, 1], &[0, 1]);
        assert!(partial_repair_needed(&state, &h(9), 0, Some(&peer)));
    }

    #[test]
    fn block_root_group_id_roundtrip() {
        let mut group_id = vec![PARTIAL_COLUMNS_VERSION_BYTE];
        group_id.extend_from_slice(&h(5));
        assert_eq!(block_root_from_group_id(&group_id), Some(h(5)));
        assert_eq!(block_root_from_group_id(&group_id[..32]), None);
        let mut bad_version = group_id.clone();
        bad_version[0] = 0x01;
        assert_eq!(block_root_from_group_id(&bad_version), None);
    }

    #[test]
    fn decode_partial_rejects_inconsistent_sparse_vectors() {
        let mut group_id = vec![PARTIAL_COLUMNS_VERSION_BYTE];
        group_id.extend_from_slice(&h(6));
        let malformed = PartialDataColumnSidecar {
            cells_present_bitmap: bitmap(1, &[0]),
            column: Vec::new(),
            kzg_proofs: Vec::new(),
            header: None,
        };
        assert!(decode_partial(0, &group_id, &malformed.encode()).is_err());
    }

    // -- Partial (custody-cell) blob data --

    #[test]
    fn insert_cells_creates_partial_and_get_cells_reads_it() {
        let mut pool = ElBlobPool::default();
        assert_eq!(pool.insert_cells(h(1), [cell(3, 9)]), 1);
        // Held column → the stored cell; absent column → None.
        let got = pool.get_cells(&h(1), &[3, 7]);
        assert_eq!(got[0].as_deref(), Some(&vec![9u8; BYTES_PER_CELL][..]));
        assert!(got[1].is_none());
        // No entry at all → all None.
        assert!(pool.get_cells(&h(2), &[3]).iter().all(Option::is_none));
    }

    #[test]
    fn insert_cells_merges_in_place_preserving_fifo() {
        let mut pool = ElBlobPool::default();
        pool.insert_full(h(0), vec![0]);
        assert_eq!(pool.insert_cells(h(1), [cell(2, 1)]), 1);
        pool.insert_full(h(2), vec![2]);
        // A second batch for h(1) merges in place: new column counts, no dup hash,
        // FIFO order stays [0, 1, 2].
        assert_eq!(pool.insert_cells(h(1), [cell(5, 1)]), 1);
        // Re-adding an already-held column adds nothing.
        assert_eq!(pool.insert_cells(h(1), [cell(5, 9)]), 0);
        assert_eq!(order(&pool), vec![0, 1, 2]);
        let got = pool.get_cells(&h(1), &[2, 5]);
        assert!(got[0].is_some() && got[1].is_some());
    }

    // A hash is fetched as either full or partial data, never both (one fetch
    // decision per blob), so mixing the two is an asserted invariant violation.
    // These are `#[should_panic]` and gated on `debug_assertions` (where
    // `debug_assert!` fires); in release the fallback path is exercised instead.

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "already held as partial cells")]
    fn insert_full_over_partial_is_rejected() {
        let mut pool = ElBlobPool::default();
        pool.insert_cells(h(1), [cell(2, 1)]);
        pool.insert_full(h(1), vec![7u8; BLOB_SIZE]);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "already held as a full blob")]
    fn insert_cells_over_full_is_rejected() {
        let mut pool = ElBlobPool::default();
        pool.insert_full(h(1), vec![7u8; BLOB_SIZE]);
        pool.insert_cells(h(1), [cell(2, 0xAB)]);
    }

    #[test]
    fn take_pending_returns_full_skips_partial() {
        let mut pool = ElBlobPool::default();
        pool.insert_full(h(0), vec![0]);
        pool.insert_cells(h(1), [cell(2, 1)]);
        pool.insert_full(h(2), vec![2]);
        // Builder takes only full blobs; the partial entry is left in place.
        let taken: Vec<u8> = pool
            .take_pending(MAX_BLOBS_PER_BLOCK, 0)
            .iter()
            .map(|(hash, _)| hash[0])
            .collect();
        assert_eq!(taken, vec![0, 2]);
        assert!(pool.contains(&h(1)));
        assert!(matches!(pool.entry(&h(1)), Some(ElBlobEntry::Partial(_))));
    }

    #[test]
    fn included_refuses_partial_reentry() {
        let mut pool = ElBlobPool::default();
        pool.mark_included(h(1), 0);
        assert_eq!(pool.insert_cells(h(1), [cell(2, 1)]), 0);
        assert!(!pool.contains(&h(1)));
    }

    #[test]
    fn capacity_counts_partial_entries_as_one_slot() {
        let mut pool = ElBlobPool::default();
        // First entry is a partial one; fill past capacity with fulls.
        pool.insert_cells(h(0), [cell(1, 0)]);
        for i in 1..=(EL_BLOB_POOL_CAPACITY as u8) {
            pool.insert_full(h(i), vec![i]);
        }
        assert_eq!(pool.pending.len(), EL_BLOB_POOL_CAPACITY);
        // The oldest (partial) entry was evicted like any full one.
        assert!(!pool.contains(&h(0)));
    }

    #[test]
    fn served_custody_cell_matches_pool_derived_cell() {
        // The custody server serves derive_cell(payload_for_blob_hash(hash), col);
        // a full-blob holder's get_cells derives the same bytes — so partial and
        // full holders agree on every (blob, column) cell.
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&random_bytes(&mut StdRng::seed_from_u64(7), 32));
        let blob = payload_for_blob_hash(&hash);
        let served = derive_cell(&blob, 42);

        let mut pool = ElBlobPool::default();
        pool.insert_full(hash, blob.clone());
        assert_eq!(
            pool.get_cells(&hash, &[42])[0].as_deref(),
            Some(&served[..])
        );
    }

    #[test]
    fn reconstruction_batches_use_one_parallel_deadline_and_overlap() {
        let config = BlobReconstructionConfig {
            delay: Duration::from_secs(10),
            trigger: BlobReconstructionTrigger::PerRow,
        };
        let mut state = PartialState::new(true, true, 7, 128, Some(config));
        state.schedule_batch(
            EligibleRows {
                block_root: h(1),
                generation: 1,
                trigger: config.trigger,
                complete_columns: 0,
                rows: vec![(0, 64), (1, 64), (2, 64)],
            },
            config,
        );
        assert_eq!(state.scheduled_reconstructions.len(), 1);
        assert_eq!(state.scheduled_reconstructions[0].rows, vec![0, 1, 2]);
        let first_deadline = state.scheduled_reconstructions[0].ready_at;

        state.schedule_batch(
            EligibleRows {
                block_root: h(2),
                generation: 2,
                trigger: config.trigger,
                complete_columns: 0,
                rows: vec![(3, 64)],
            },
            config,
        );
        let second_deadline = state
            .scheduled_reconstructions
            .iter()
            .find(|batch| batch.block_root == h(2))
            .unwrap()
            .ready_at;
        assert!(second_deadline >= first_deadline);
        assert!(second_deadline.duration_since(first_deadline) < config.delay);
    }

    #[test]
    fn zero_delay_is_immediately_due_and_disabled_state_has_no_config() {
        let config = BlobReconstructionConfig {
            delay: Duration::ZERO,
            trigger: BlobReconstructionTrigger::CompleteColumns,
        };
        let mut state = PartialState::new(true, true, 7, 128, Some(config));
        state.schedule_batch(
            EligibleRows {
                block_root: h(3),
                generation: 3,
                trigger: config.trigger,
                complete_columns: 64,
                rows: vec![(0, 64)],
            },
            config,
        );
        assert!(state.scheduled_reconstructions[0].ready_at <= Instant::now());

        let disabled = PartialState::new(false, true, 7, 128, None);
        assert!(disabled.reconstruction.is_none());
        assert!(disabled.scheduled_reconstructions.is_empty());
    }

    #[test]
    fn queue_capacity_bounds_total_rows_not_batches() {
        let config = BlobReconstructionConfig {
            delay: Duration::from_secs(10),
            trigger: BlobReconstructionTrigger::PerRow,
        };
        let mut state = PartialState::new(true, true, 7, 128, Some(config));
        // Fill the queue to one row below capacity.
        let near_full: Vec<(usize, usize)> = (0..MAX_SCHEDULED_RECONSTRUCTION_ROWS - 1)
            .map(|r| (r, 64))
            .collect();
        state.schedule_batch(
            EligibleRows {
                block_root: h(1),
                generation: 1,
                trigger: config.trigger,
                complete_columns: 0,
                rows: near_full,
            },
            config,
        );
        // A 3-row batch has room for only 1 row; the other 2 are dropped.
        state.schedule_batch(
            EligibleRows {
                block_root: h(2),
                generation: 2,
                trigger: config.trigger,
                complete_columns: 0,
                rows: vec![(0, 64), (1, 64), (2, 64)],
            },
            config,
        );
        let queued: usize = state
            .scheduled_reconstructions
            .iter()
            .map(|batch| batch.rows.len())
            .sum();
        assert_eq!(queued, MAX_SCHEDULED_RECONSTRUCTION_ROWS);
        let second = state
            .scheduled_reconstructions
            .iter()
            .find(|batch| batch.block_root == h(2))
            .expect("second batch keeps its admitted row");
        assert_eq!(second.rows, vec![0]);
    }

    #[test]
    fn attempts_are_unique_per_scheduled_batch() {
        let config = BlobReconstructionConfig {
            delay: Duration::from_secs(1),
            trigger: BlobReconstructionTrigger::PerRow,
        };
        let mut state = PartialState::new(true, true, 7, 128, Some(config));
        for (gen, row) in [(1u64, 0usize), (2, 1)] {
            state.schedule_batch(
                EligibleRows {
                    block_root: h(gen as u8),
                    generation: gen,
                    trigger: config.trigger,
                    complete_columns: 0,
                    rows: vec![(row, 64)],
                },
                config,
            );
        }
        let attempts: Vec<u64> = state
            .scheduled_reconstructions
            .iter()
            .map(|batch| batch.attempt)
            .collect();
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0], attempts[1]);
    }
}
