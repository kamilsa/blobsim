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
    CellBitmap, DataColumnSidecar, PartialDataColumn, PartialDataColumnHeader,
    PartialDataColumnPartsMetadata, PartialDataColumnSidecar,
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
struct BlockAssembly {
    header: Option<PartialDataColumnHeader>,
    /// Current merged partial per column index.
    columns: HashMap<u64, PartialDataColumnSidecar>,
    complete: HashSet<u64>,
}

/// Accumulates received partial columns per block (bounded LRU by block root)
/// and promotes columns to full sidecars once every cell is present.
pub struct PartialColumnAssembler {
    capacity: usize,
    order: VecDeque<[u8; 32]>,
    blocks: HashMap<[u8; 32], BlockAssembly>,
}

impl PartialColumnAssembler {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            blocks: HashMap::new(),
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
            self.blocks.insert(
                block_root,
                BlockAssembly {
                    header: None,
                    columns: HashMap::new(),
                    complete: HashSet::new(),
                },
            );
        }
        self.blocks.get_mut(&block_root).expect("just inserted")
    }

    /// Record a block's header (idempotent; keeps the first non-None header).
    pub fn set_header(&mut self, block_root: [u8; 32], header: PartialDataColumnHeader) {
        let block = self.ensure_block(block_root);
        if block.header.is_none() {
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
        if let Some(header) = &partial.sidecar.header {
            self.set_header(partial.block_root, header.clone());
        }
        let block = self.ensure_block(partial.block_root);
        let num_blobs = partial.sidecar.cells_present_bitmap.len();

        let entry = block
            .columns
            .entry(partial.index)
            .or_insert_with(|| PartialDataColumnSidecar::empty(num_blobs, None));

        let before = entry.num_present();
        *entry = entry.merge(&partial.sidecar);
        let after = entry.num_present();
        let added_cells = after.saturating_sub(before);

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
