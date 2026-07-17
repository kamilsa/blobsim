//! Gossipsub 1.3 partial-message glue for data column sidecars.
//!
//! This module ports Lighthouse's partial-column machinery onto the simulator's
//! types:
//!   - [`OutgoingPartialColumn`] implements the gossipsub [`Partial`] trait, which
//!     the behaviour calls per mesh peer to decide what cells to send (the
//!     three-phase header → metadata → delta flow).
//!   - [`MaybeKnownMetadata`] implements the gossipsub [`Metadata`] trait: the
//!     per-peer `available` / `requests` bitmaps, merged (union'd) across rounds.
//!   - [`PartialColumnHeaderTracker`] ensures the block header is sent to each
//!     peer at most once per block.
//!   - [`PartialColumnAssembler`] accumulates received cells per (block, column)
//!     and promotes a column to a full [`DataColumnSidecar`] once complete.
//!
//! See `Cargo.toml` for why `libp2p` is pinned to a rust-libp2p master rev (the
//! `partial-messages` feature is not yet on crates.io).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use libp2p::gossipsub::partial_messages::{Metadata, Partial, PartialAction, PartialError};
use libp2p::PeerId;

use crate::types::{
    blob_from_commitment, blob_hash_from_commitment, derive_cell, BlobReconstructionTrigger,
    CellBitmap, DataColumnSidecar, PartialDataColumn, PartialDataColumnHeader,
    PartialDataColumnPartsMetadata, PartialDataColumnSidecar, KZG_ELEMENT_SIZE,
    NUM_CUSTODY_COLUMNS,
};

/// Version byte prefixing a partial column group id (`0x00 || block_root`).
pub const PARTIAL_COLUMNS_VERSION_BYTE: u8 = 0x00;

/// The set of peers we have already sent a header-only message to, for one block.
pub type HeaderSentSet = Arc<Mutex<HashSet<PeerId>>>;

// ---------------------------------------------------------------------------
// Per-peer metadata
// ---------------------------------------------------------------------------

/// Per-peer partial metadata, tracked as either not-yet-known or known.
///
/// Implements the gossipsub [`Metadata`] trait. `update` merges a peer's
/// advertised `available`/`requests` bitmaps into our view (union — it only
/// grows). `update_from_data` folds in what a peer learns from a full sidecar we
/// sent it.
#[derive(Debug, Clone)]
pub enum MaybeKnownMetadata {
    Unknown,
    Known {
        metadata: PartialDataColumnPartsMetadata,
        encoded: Vec<u8>,
    },
}

impl MaybeKnownMetadata {
    fn known(metadata: PartialDataColumnPartsMetadata) -> Self {
        let encoded = metadata.encode();
        MaybeKnownMetadata::Known { metadata, encoded }
    }

    /// Union `incoming` into our current metadata, returning whether anything
    /// changed. Bitmaps only ever grow (monotonic).
    fn do_update(&mut self, incoming: PartialDataColumnPartsMetadata) -> bool {
        match self {
            MaybeKnownMetadata::Unknown => {
                *self = MaybeKnownMetadata::known(incoming);
                true
            }
            MaybeKnownMetadata::Known { metadata, .. } => {
                let merged = PartialDataColumnPartsMetadata {
                    available: metadata.available.union(&incoming.available),
                    requests: metadata.requests.union(&incoming.requests),
                };
                if merged == *metadata {
                    false
                } else {
                    *self = MaybeKnownMetadata::known(merged);
                    true
                }
            }
        }
    }
}

impl From<PartialDataColumnPartsMetadata> for MaybeKnownMetadata {
    fn from(metadata: PartialDataColumnPartsMetadata) -> Self {
        MaybeKnownMetadata::known(metadata)
    }
}

impl Metadata for MaybeKnownMetadata {
    fn as_slice(&self) -> &[u8] {
        match self {
            MaybeKnownMetadata::Unknown => &[],
            MaybeKnownMetadata::Known { encoded, .. } => encoded,
        }
    }

    fn update(&mut self, data: &[u8]) -> Result<bool, PartialError> {
        let received = PartialDataColumnPartsMetadata::decode(data)
            .map_err(|_| PartialError::InvalidFormat)?;
        Ok(self.do_update(received))
    }

    fn update_from_data(&mut self, data: &[u8]) -> Result<(), PartialError> {
        if data.is_empty() {
            return Ok(());
        }
        let sidecar =
            PartialDataColumnSidecar::decode(data).map_err(|_| PartialError::InvalidFormat)?;
        // A peer that received these cells now both has them and (implicitly)
        // requested them.
        self.do_update(PartialDataColumnPartsMetadata {
            available: sidecar.cells_present_bitmap.clone(),
            requests: sidecar.cells_present_bitmap,
        });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Outgoing partial column (implements the gossipsub Partial trait)
// ---------------------------------------------------------------------------

/// A data column we are publishing via the partial protocol. Wraps the cells we
/// currently hold plus the header (sent once per peer), and answers the
/// behaviour's per-peer "what should I send this peer?" query.
#[derive(Debug, Clone)]
pub struct OutgoingPartialColumn {
    partial_column: Arc<PartialDataColumn>,
    metadata: MaybeKnownMetadata,
    /// Encoded header-only sidecar (empty cells + header), sent in phase 1.
    header_message: Vec<u8>,
    header_sent_set: HeaderSentSet,
}

impl OutgoingPartialColumn {
    /// Build an outgoing column. `requests` are the cells we still want from
    /// peers; we always also mark the cells we already hold as requested so our
    /// advertised `requests ⊇ available` (matches Lighthouse).
    pub fn new(
        partial_column: Arc<PartialDataColumn>,
        header: &PartialDataColumnHeader,
        header_sent_set: HeaderSentSet,
        requests: CellBitmap,
    ) -> Self {
        let available = partial_column.sidecar.cells_present_bitmap.clone();
        let requests = requests.union(&available);
        let metadata = MaybeKnownMetadata::from(PartialDataColumnPartsMetadata {
            available: available.clone(),
            requests,
        });

        // Header-only message: same length bitmap but zero cells, header attached.
        let header_message =
            PartialDataColumnSidecar::empty(available.len(), Some(header.clone())).encode();

        Self {
            partial_column,
            metadata,
            header_message,
            header_sent_set,
        }
    }
}

impl Partial for OutgoingPartialColumn {
    fn group_id(&self) -> Vec<u8> {
        let mut id = Vec::with_capacity(1 + 32);
        id.push(PARTIAL_COLUMNS_VERSION_BYTE);
        id.extend_from_slice(&self.partial_column.block_root);
        id
    }

    fn metadata(&self) -> Box<dyn Metadata> {
        Box::new(self.metadata.clone())
    }

    fn partial_action_from_metadata(
        &self,
        peer_id: PeerId,
        metadata: Option<&[u8]>,
    ) -> Result<PartialAction, PartialError> {
        match metadata {
            // Phase 1: peer has no metadata yet → send header-only, at most once.
            None => {
                let send = self
                    .header_sent_set
                    .lock()
                    .expect("header set poisoned")
                    .insert(peer_id)
                    .then(|| {
                        (
                            self.header_message.clone(),
                            Box::new(MaybeKnownMetadata::Unknown) as Box<dyn Metadata>,
                        )
                    });
                Ok(PartialAction { need: false, send })
            }
            // Empty metadata: nothing to do.
            Some([]) => Ok(PartialAction {
                need: false,
                send: None,
            }),
            // Phase 3: peer advertised metadata → send the cells it wants but lacks.
            Some(bytes) => {
                self.header_sent_set
                    .lock()
                    .expect("header set poisoned")
                    .insert(peer_id);

                let peer_meta = PartialDataColumnPartsMetadata::decode(bytes)
                    .map_err(|_| PartialError::InvalidFormat)?;

                let ours = &self.partial_column.sidecar.cells_present_bitmap;
                // We need data back if the peer has cells we don't.
                let need = !peer_meta.available.is_subset(ours);

                // want = requests − available (cells the peer wants and lacks).
                let want = peer_meta.requests.difference(&peer_meta.available);
                let filtered = self.partial_column.sidecar.filter(|idx| want.get(idx));

                let send = if filtered.num_present() == 0 {
                    None
                } else {
                    let updated = PartialDataColumnPartsMetadata {
                        available: peer_meta.available.union(&filtered.cells_present_bitmap),
                        requests: peer_meta.requests.union(&filtered.cells_present_bitmap),
                    };
                    Some((
                        filtered.encode(),
                        Box::new(MaybeKnownMetadata::from(updated)) as Box<dyn Metadata>,
                    ))
                };

                Ok(PartialAction { need, send })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Header tracker (header sent at most once per peer per block)
// ---------------------------------------------------------------------------

/// Bounded map from block root to its shared [`HeaderSentSet`], so all column
/// topics for a block share one "have we sent this peer the header?" set.
/// Evicts the oldest block once `capacity` is exceeded.
pub struct PartialColumnHeaderTracker {
    capacity: usize,
    order: VecDeque<[u8; 32]>,
    sets: HashMap<[u8; 32], HeaderSentSet>,
}

impl PartialColumnHeaderTracker {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            sets: HashMap::new(),
        }
    }

    /// Fetch (or create) the shared header-sent set for a block.
    pub fn get_for_block(&mut self, block_root: [u8; 32]) -> HeaderSentSet {
        if let Some(set) = self.sets.get(&block_root) {
            return Arc::clone(set);
        }
        if self.order.len() >= self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.sets.remove(&old);
            }
        }
        let set: HeaderSentSet = Arc::new(Mutex::new(HashSet::new()));
        self.sets.insert(block_root, Arc::clone(&set));
        self.order.push_back(block_root);
        set
    }
}

// ---------------------------------------------------------------------------
// Assembler (merge received partials into complete columns)
// ---------------------------------------------------------------------------

/// Outcome of merging one received partial column.
pub struct MergeResult {
    /// Number of genuinely new cells added by this partial.
    pub added_cells: usize,
    /// The full column, if this merge just completed it.
    pub newly_complete: Option<DataColumnSidecar>,
}

/// Per-block assembly state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowState {
    Collecting,
    Reconstructing,
    Finished,
    Unrecoverable,
}

/// Atomic set of rows newly made eligible by one ingestion unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EligibleRows {
    pub block_root: [u8; 32],
    pub generation: u64,
    pub trigger: BlobReconstructionTrigger,
    pub complete_columns: usize,
    /// `(blob_index, cells_held_at_start)` in ascending row order.
    pub rows: Vec<(usize, usize)>,
}

/// Per-row result of applying a due reconstruction batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconstructedRow {
    pub blob_index: usize,
    pub cells_added: usize,
    pub columns_updated: usize,
    pub already_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconstructionApply {
    Stale,
    Applied {
        rows: Vec<ReconstructedRow>,
        changed_columns: Vec<u64>,
        newly_complete_columns: usize,
    },
}

struct BlockAssembly {
    generation: u64,
    header: Option<PartialDataColumnHeader>,
    /// Current merged partial per column index.
    columns: HashMap<u64, PartialDataColumnSidecar>,
    complete: HashSet<u64>,
    row_counts: Vec<usize>,
    row_states: Vec<RowState>,
}

/// Accumulates received partial columns per block (bounded LRU by block root)
/// and promotes columns to full sidecars once every cell is present.
pub struct PartialColumnAssembler {
    capacity: usize,
    order: VecDeque<[u8; 32]>,
    blocks: HashMap<[u8; 32], BlockAssembly>,
    next_generation: u64,
}

impl PartialColumnAssembler {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            blocks: HashMap::new(),
            next_generation: 1,
        }
    }

    fn ensure_block(&mut self, block_root: [u8; 32]) -> &mut BlockAssembly {
        if !self.blocks.contains_key(&block_root) {
            if self.order.len() >= self.capacity {
                if let Some(old) = self.order.pop_front() {
                    self.blocks.remove(&old);
                }
            }
            self.order.push_back(block_root);
            let generation = self.next_generation;
            self.next_generation = self.next_generation.wrapping_add(1).max(1);
            self.blocks.insert(
                block_root,
                BlockAssembly {
                    generation,
                    header: None,
                    columns: HashMap::new(),
                    complete: HashSet::new(),
                    row_counts: Vec::new(),
                    row_states: Vec::new(),
                },
            );
        }
        self.blocks.get_mut(&block_root).expect("just inserted")
    }

    /// Record a block's header (idempotent; keeps the first non-None header).
    pub fn set_header(&mut self, block_root: [u8; 32], header: PartialDataColumnHeader) {
        let block = self.ensure_block(block_root);
        if block.header.is_none() {
            let rows = header.kzg_commitments.len();
            // Cells may precede the header, but their bitmap length must agree
            // with the commitment count before they can participate in a row.
            block
                .columns
                .retain(|_, column| column.cells_present_bitmap.len() == rows);
            block.complete = block
                .columns
                .iter()
                .filter_map(|(&index, column)| column.is_complete().then_some(index))
                .collect();
            block.row_counts = (0..rows)
                .map(|row| {
                    block
                        .columns
                        .values()
                        .filter(|column| column.cells_present_bitmap.get(row))
                        .count()
                })
                .collect();
            block.row_states = block
                .row_counts
                .iter()
                .map(|&count| {
                    if count >= NUM_CUSTODY_COLUMNS as usize {
                        RowState::Finished
                    } else {
                        RowState::Collecting
                    }
                })
                .collect();
            block.header = Some(header);
        }
    }

    /// Fetch a clone of the block's header, if known.
    pub fn get_header(&self, block_root: &[u8; 32]) -> Option<PartialDataColumnHeader> {
        self.blocks.get(block_root)?.header.clone()
    }

    /// Roots of all currently-tracked blocks whose header is known, oldest
    /// first. Used to re-check custody columns when the local EL blob pool
    /// grows (a new blob may belong to any known block).
    pub fn blocks_with_header(&self) -> Vec<[u8; 32]> {
        self.order
            .iter()
            .filter(|root| {
                self.blocks
                    .get(*root)
                    .map(|b| b.header.is_some())
                    .unwrap_or(false)
            })
            .copied()
            .collect()
    }

    /// Merge a received partial column into the assembly. Records the block's
    /// header if the partial carries one.
    pub fn merge_partial(&mut self, partial: &PartialDataColumn) -> MergeResult {
        if !partial.sidecar.is_structurally_valid() {
            return MergeResult {
                added_cells: 0,
                newly_complete: None,
            };
        }
        if let Some(header) = &partial.sidecar.header {
            if header.kzg_commitments.len() != partial.sidecar.cells_present_bitmap.len() {
                return MergeResult {
                    added_cells: 0,
                    newly_complete: None,
                };
            }
            self.set_header(partial.block_root, header.clone());
        }
        let block = self.ensure_block(partial.block_root);
        let num_blobs = partial.sidecar.cells_present_bitmap.len();
        if block
            .header
            .as_ref()
            .is_some_and(|header| header.kzg_commitments.len() != num_blobs)
        {
            return MergeResult {
                added_cells: 0,
                newly_complete: None,
            };
        }

        let entry = block
            .columns
            .entry(partial.index)
            .or_insert_with(|| PartialDataColumnSidecar::empty(num_blobs, None));

        let new_rows: Vec<usize> = (0..num_blobs)
            .filter(|&row| {
                partial.sidecar.cells_present_bitmap.get(row)
                    && !entry.cells_present_bitmap.get(row)
            })
            .collect();
        let before = entry.num_present();
        *entry = entry.merge(&partial.sidecar);
        let after = entry.num_present();
        let added_cells = after.saturating_sub(before);
        if block.header.is_some() {
            for row in new_rows {
                if let Some(count) = block.row_counts.get_mut(row) {
                    *count += 1;
                    if *count >= NUM_CUSTODY_COLUMNS as usize {
                        if let Some(state) = block.row_states.get_mut(row) {
                            *state = RowState::Finished;
                        }
                    }
                }
            }
        }

        let newly_complete = if entry.is_complete() && !block.complete.contains(&partial.index) {
            block.complete.insert(partial.index);
            block.header.as_ref().and_then(|header| {
                PartialDataColumn {
                    block_root: partial.block_root,
                    index: partial.index,
                    sidecar: entry.clone(),
                }
                .try_clone_full(header)
            })
        } else {
            None
        };

        MergeResult {
            added_cells,
            newly_complete,
        }
    }

    /// Atomically mark newly eligible rows as reconstructing. Call this only
    /// after a complete network/local-pool ingestion unit.
    pub fn take_eligible_rows(
        &mut self,
        block_root: [u8; 32],
        trigger: BlobReconstructionTrigger,
    ) -> Option<EligibleRows> {
        let block = self.blocks.get_mut(&block_root)?;
        let header = block.header.as_ref()?;
        let complete_columns = block.complete.len();
        if trigger == BlobReconstructionTrigger::CompleteColumns
            && complete_columns < crate::types::CELLS_PER_BLOB
        {
            return None;
        }
        let mut rows = Vec::new();
        for row in 0..header.kzg_commitments.len() {
            let count = block.row_counts.get(row).copied().unwrap_or(0);
            if block.row_states.get(row) != Some(&RowState::Collecting)
                || count >= NUM_CUSTODY_COLUMNS as usize
                || (trigger == BlobReconstructionTrigger::PerRow
                    && count < crate::types::CELLS_PER_BLOB)
            {
                continue;
            }
            if blob_hash_from_commitment(&header.kzg_commitments[row]).is_none() {
                block.row_states[row] = RowState::Unrecoverable;
                continue;
            }
            block.row_states[row] = RowState::Reconstructing;
            rows.push((row, count));
        }
        (!rows.is_empty()).then_some(EligibleRows {
            block_root,
            generation: block.generation,
            trigger,
            complete_columns,
            rows,
        })
    }

    /// Apply a due batch if its generation still names the same assembly.
    pub fn apply_reconstruction(
        &mut self,
        block_root: [u8; 32],
        generation: u64,
        rows: &[usize],
    ) -> ReconstructionApply {
        let Some(block) = self.blocks.get_mut(&block_root) else {
            return ReconstructionApply::Stale;
        };
        if block.generation != generation {
            return ReconstructionApply::Stale;
        }
        let Some(header) = block.header.as_ref() else {
            return ReconstructionApply::Stale;
        };
        let complete_before = block.complete.len();
        let mut changed = HashSet::new();
        let mut results = Vec::new();
        for &row in rows {
            if row >= block.row_states.len() {
                continue;
            }
            let already_complete = block.row_counts[row] >= NUM_CUSTODY_COLUMNS as usize;
            if already_complete {
                block.row_states[row] = RowState::Finished;
                results.push(ReconstructedRow {
                    blob_index: row,
                    cells_added: 0,
                    columns_updated: 0,
                    already_complete: true,
                });
                continue;
            }
            if block.row_states[row] != RowState::Reconstructing {
                continue;
            }
            let Some(blob) = blob_from_commitment(&header.kzg_commitments[row]) else {
                block.row_states[row] = RowState::Unrecoverable;
                continue;
            };
            let mut added = 0;
            for column_index in 0..NUM_CUSTODY_COLUMNS {
                let sidecar = block.columns.entry(column_index).or_insert_with(|| {
                    PartialDataColumnSidecar::empty(header.kzg_commitments.len(), None)
                });
                if sidecar.insert_missing(
                    row,
                    derive_cell(&blob, column_index),
                    vec![0xEE; KZG_ELEMENT_SIZE],
                ) {
                    added += 1;
                    changed.insert(column_index);
                    block.row_counts[row] += 1;
                }
                if sidecar.is_complete() {
                    block.complete.insert(column_index);
                }
            }
            block.row_states[row] = RowState::Finished;
            results.push(ReconstructedRow {
                blob_index: row,
                cells_added: added,
                columns_updated: added,
                already_complete: false,
            });
        }
        let mut changed_columns: Vec<u64> = changed.into_iter().collect();
        changed_columns.sort_unstable();
        ReconstructionApply::Applied {
            rows: results,
            changed_columns,
            newly_complete_columns: block.complete.len().saturating_sub(complete_before),
        }
    }

    /// Return rows to collecting when a bounded scheduler cannot retain a job.
    pub fn release_reconstructing(
        &mut self,
        block_root: [u8; 32],
        generation: u64,
        rows: &[usize],
    ) {
        let Some(block) = self.blocks.get_mut(&block_root) else {
            return;
        };
        if block.generation != generation {
            return;
        }
        for &row in rows {
            if block.row_states.get(row) == Some(&RowState::Reconstructing) {
                block.row_states[row] = RowState::Collecting;
            }
        }
    }

    /// Whether every column in `custody` has fully assembled for this block — i.e.
    /// the node's entire custody set is complete. False if the block is unknown or
    /// `custody` is empty.
    pub fn custody_set_complete(&self, block_root: &[u8; 32], custody: &[u64]) -> bool {
        if custody.is_empty() {
            return false;
        }
        match self.blocks.get(block_root) {
            Some(block) => custody.iter().all(|c| block.complete.contains(c)),
            None => false,
        }
    }

    /// Custody-cell possession snapshot for a block: how many of the node's
    /// `custody` columns' cells are already present locally, and the block's blob
    /// count (= cells per column). Returns `(cells_held, n_blobs)`; `n_blobs` is 0
    /// when the block's header is not yet known (nothing to possess).
    pub fn custody_possession(&self, block_root: &[u8; 32], custody: &[u64]) -> (usize, usize) {
        let Some(block) = self.blocks.get(block_root) else {
            return (0, 0);
        };
        let n_blobs = block
            .header
            .as_ref()
            .map(|h| h.kzg_commitments.len())
            .unwrap_or(0);
        let held: usize = custody
            .iter()
            .filter_map(|c| block.columns.get(c))
            .map(|col| col.num_present())
            .sum();
        (held, n_blobs)
    }

    /// Snapshot the current partial for a (block, column), for re-publishing our
    /// accumulated cells as a delta.
    pub fn current_partial(&self, block_root: &[u8; 32], index: u64) -> Option<PartialDataColumn> {
        let block = self.blocks.get(block_root)?;
        let sidecar = block.columns.get(&index)?.clone();
        Some(PartialDataColumn {
            block_root: *block_root,
            index,
            sidecar,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        commitment_for_blob_hash, payload_for_blob_hash, PartialDataColumnHeader, BYTES_PER_CELL,
    };

    fn root(n: u8) -> [u8; 32] {
        let mut root = [0; 32];
        root[0] = n;
        root
    }

    fn setup(capacity: usize, root: [u8; 32], hashes: &[[u8; 32]]) -> PartialColumnAssembler {
        let mut assembler = PartialColumnAssembler::new(capacity);
        assembler.set_header(
            root,
            PartialDataColumnHeader::from_commitments(
                hashes.iter().map(commitment_for_blob_hash).collect(),
            ),
        );
        assembler
    }

    fn merge_rows(
        assembler: &mut PartialColumnAssembler,
        root: [u8; 32],
        column: u64,
        hashes: &[[u8; 32]],
        rows: &[usize],
    ) -> usize {
        let mut sidecar = PartialDataColumnSidecar::empty(hashes.len(), None);
        for &row in rows {
            sidecar.insert_missing(
                row,
                derive_cell(&payload_for_blob_hash(&hashes[row]), column),
                vec![0xEE; KZG_ELEMENT_SIZE],
            );
        }
        assembler
            .merge_partial(&PartialDataColumn {
                block_root: root,
                index: column,
                sidecar,
            })
            .added_cells
    }

    #[test]
    fn counts_only_new_cells_and_complete_columns_triggers_at_64() {
        let hashes = [[1; 32], [2; 32]];
        let block = root(1);
        let mut assembler = setup(4, block, &hashes);
        let malformed = PartialDataColumn {
            block_root: block,
            index: 127,
            sidecar: PartialDataColumnSidecar::empty(3, None),
        };
        assert_eq!(assembler.merge_partial(&malformed).added_cells, 0);
        assert!(assembler.current_partial(&block, 127).is_none());
        for column in 0..63 {
            assert_eq!(
                merge_rows(&mut assembler, block, column, &hashes, &[0, 1]),
                2
            );
        }
        assert_eq!(merge_rows(&mut assembler, block, 0, &hashes, &[0, 1]), 0);
        assert!(assembler
            .take_eligible_rows(block, BlobReconstructionTrigger::CompleteColumns)
            .is_none());
        merge_rows(&mut assembler, block, 63, &hashes, &[0, 1]);
        let batch = assembler
            .take_eligible_rows(block, BlobReconstructionTrigger::CompleteColumns)
            .expect("64 complete columns trigger all rows");
        assert_eq!(batch.rows, vec![(0, 64), (1, 64)]);
        assert!(assembler
            .take_eligible_rows(block, BlobReconstructionTrigger::CompleteColumns)
            .is_none());
    }

    #[test]
    fn per_row_batches_can_start_independently_and_reconstruct_exact_cells() {
        let hashes = [[3; 32], [4; 32]];
        let block = root(2);
        let mut assembler = setup(4, block, &hashes);
        for column in 0..64 {
            merge_rows(&mut assembler, block, column, &hashes, &[0]);
        }
        let first = assembler
            .take_eligible_rows(block, BlobReconstructionTrigger::PerRow)
            .expect("first row eligible");
        assert_eq!(first.rows, vec![(0, 64)]);
        for column in 0..64 {
            merge_rows(&mut assembler, block, column, &hashes, &[1]);
        }
        let second = assembler
            .take_eligible_rows(block, BlobReconstructionTrigger::PerRow)
            .expect("second row eligible later");
        assert_eq!(second.rows, vec![(1, 64)]);

        let ReconstructionApply::Applied { rows, .. } =
            assembler.apply_reconstruction(block, first.generation, &[0])
        else {
            panic!("current generation");
        };
        assert_eq!(rows[0].cells_added, 64);
        for column in 0..NUM_CUSTODY_COLUMNS {
            let partial = assembler.current_partial(&block, column).unwrap();
            assert_eq!(
                partial.sidecar.get(0).unwrap().0,
                &derive_cell(&payload_for_blob_hash(&hashes[0]), column)
            );
        }
    }

    #[test]
    fn late_cells_are_preserved_and_natural_completion_adds_zero() {
        let hashes = [[5; 32]];
        let block = root(3);
        let mut assembler = setup(4, block, &hashes);
        for column in 0..64 {
            merge_rows(&mut assembler, block, column, &hashes, &[0]);
        }
        let batch = assembler
            .take_eligible_rows(block, BlobReconstructionTrigger::PerRow)
            .unwrap();
        for column in 64..NUM_CUSTODY_COLUMNS {
            merge_rows(&mut assembler, block, column, &hashes, &[0]);
        }
        let ReconstructionApply::Applied {
            rows,
            changed_columns,
            ..
        } = assembler.apply_reconstruction(block, batch.generation, &[0])
        else {
            panic!("current generation");
        };
        assert!(rows[0].already_complete);
        assert_eq!(rows[0].cells_added, 0);
        assert!(changed_columns.is_empty());
        assert_eq!(assembler.custody_possession(&block, &[0]).0, 1);
        assert_eq!(
            assembler.current_partial(&block, 0).unwrap().sidecar.column[0].len(),
            BYTES_PER_CELL
        );
    }

    #[test]
    fn stale_generation_and_multiple_roots_are_isolated() {
        let hashes = [[6; 32]];
        let mut assembler = setup(1, root(4), &hashes);
        for column in 0..64 {
            merge_rows(&mut assembler, root(4), column, &hashes, &[0]);
        }
        let old = assembler
            .take_eligible_rows(root(4), BlobReconstructionTrigger::PerRow)
            .unwrap();
        assembler.set_header(
            root(5),
            PartialDataColumnHeader::from_commitments(vec![commitment_for_blob_hash(&hashes[0])]),
        );
        assembler.set_header(
            root(4),
            PartialDataColumnHeader::from_commitments(vec![commitment_for_blob_hash(&hashes[0])]),
        );
        assert_eq!(
            assembler.apply_reconstruction(root(4), old.generation, &[0]),
            ReconstructionApply::Stale
        );
        assert!(assembler.current_partial(&root(4), 0).is_none());
    }

    #[test]
    fn structurally_invalid_partial_is_not_merged() {
        let hashes = [[7; 32]];
        let block = root(7);
        let mut assembler = setup(4, block, &hashes);
        // Bitmap claims a present cell, but the sparse vectors are empty.
        let malformed = PartialDataColumn {
            block_root: block,
            index: 0,
            sidecar: PartialDataColumnSidecar {
                cells_present_bitmap: {
                    let mut b = CellBitmap::with_len(1);
                    b.set(0);
                    b
                },
                column: Vec::new(),
                kzg_proofs: Vec::new(),
                header: None,
            },
        };
        assert_eq!(assembler.merge_partial(&malformed).added_cells, 0);
        // Neither the column nor the row count advanced from the bogus bitmap.
        assert!(assembler.current_partial(&block, 0).is_none());
        assert_eq!(assembler.custody_possession(&block, &[0]).0, 0);
    }

    #[test]
    fn malformed_header_does_not_poison_block() {
        let hash = [8; 32];
        let block = root(8);
        let mut assembler = PartialColumnAssembler::new(4);
        let malformed = PartialDataColumn {
            block_root: block,
            index: 0,
            sidecar: PartialDataColumnSidecar::empty(
                1,
                Some(PartialDataColumnHeader::from_commitments(vec![
                    commitment_for_blob_hash(&hash),
                    commitment_for_blob_hash(&hash),
                ])),
            ),
        };
        assert_eq!(assembler.merge_partial(&malformed).added_cells, 0);
        assert!(assembler.get_header(&block).is_none());

        let valid = PartialDataColumn {
            block_root: block,
            index: 0,
            sidecar: PartialDataColumnSidecar::empty(
                1,
                Some(PartialDataColumnHeader::from_commitments(vec![
                    commitment_for_blob_hash(&hash),
                ])),
            ),
        };
        assert_eq!(assembler.merge_partial(&valid).added_cells, 0);
        assert_eq!(
            assembler.get_header(&block).unwrap().kzg_commitments.len(),
            1
        );
    }
}
