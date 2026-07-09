//! Mock Ethereum types for blob propagation simulation.
//!
//! All cryptographic fields (KZG commitments, BLS signatures, proofs) are replaced
//! with `Vec<u8>` dummy byte vectors to avoid CPU overhead inside Shadow and to
//! stay within serde's default array-size support.

use alloy_rlp::{Bytes, Decodable, RlpDecodable, RlpEncodable};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

// ---------------------------------------------------------------------------
// Node roles
// ---------------------------------------------------------------------------

/// Individual role a node can hold, configured via CLI `--role` flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Proposes the beacon block at t=0, committing to the blobs it received
    /// over EL networking. In this simplified model a proposer is also a builder.
    Proposer,
    /// Releases payloads + blobs at t=4-6s. Announces blob hashes at t=0.
    Builder,
    /// Non-builder CL node. For each announced blob it independently behaves as
    /// a sampler with 85% probability or a provider with 15% probability.
    Validator,
    /// A CL attester that verifies blocks via zkEVM proofs (EIP-8142) and so does
    /// not need the execution payload envelope: it does **not** subscribe to the
    /// payload-envelope topic and instead receives only the payload-blob cells for
    /// the columns it custodies over the column subnets — i.e. partial payload data
    /// (a non-supernode never reconstructs the full payload; supernodes deferred).
    /// Combined with `Validator`; off by default.
    ZkAttester,
    /// EL-only blob load generator: originates and serves blobs at a configurable
    /// per-slot rate, paced across the slot. Holds no CL roles.
    BlobSpammer,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proposer => write!(f, "proposer"),
            Self::Builder => write!(f, "builder"),
            Self::Validator => write!(f, "validator"),
            Self::ZkAttester => write!(f, "zk-attester"),
            Self::BlobSpammer => write!(f, "blob-spammer"),
        }
    }
}

impl std::str::FromStr for Role {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "proposer" => Ok(Self::Proposer),
            "builder" => Ok(Self::Builder),
            "validator" => Ok(Self::Validator),
            "zk-attester" | "zk_attester" | "zk" => Ok(Self::ZkAttester),
            "blob-spammer" | "blob_spammer" | "spammer" => Ok(Self::BlobSpammer),
            other => Err(format!("unknown role: {other}")),
        }
    }
}

/// Set of roles a single node holds.
#[derive(Debug, Clone)]
pub struct NodeRoles {
    pub proposer: bool,
    pub builder: bool,
    pub validator: bool,
    pub zk_attester: bool,
    pub blob_spammer: bool,
}

impl NodeRoles {
    /// Build from a list of CLI-provided roles.
    pub fn from_roles(roles: &[Role]) -> Self {
        let mut nr = Self {
            proposer: false,
            builder: false,
            validator: false,
            zk_attester: false,
            blob_spammer: false,
        };
        for r in roles {
            match r {
                Role::Proposer => nr.proposer = true,
                Role::Builder => nr.builder = true,
                Role::Validator => nr.validator = true,
                Role::ZkAttester => nr.zk_attester = true,
                Role::BlobSpammer => nr.blob_spammer = true,
            }
        }
        let has_cl = nr.proposer || nr.builder || nr.validator || nr.zk_attester;
        assert!(
            !(nr.blob_spammer && has_cl),
            "blob-spammer is EL-only and cannot be combined with CL roles"
        );
        nr
    }

    pub fn is_proposer(&self) -> bool {
        self.proposer
    }
    pub fn is_builder(&self) -> bool {
        self.builder
    }
    /// A zk-attester (EIP-8142): verifies via zkEVM proofs and therefore skips the
    /// payload-envelope topic, relying on payload-blobs over the column subnets.
    pub fn is_zk_attester(&self) -> bool {
        self.zk_attester
    }
    pub fn is_blob_spammer(&self) -> bool {
        self.blob_spammer
    }
}

impl fmt::Display for NodeRoles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if self.proposer {
            parts.push("proposer");
        }
        if self.builder {
            parts.push("builder");
        }
        if self.validator {
            parts.push("validator");
        }
        if self.zk_attester {
            parts.push("zk-attester");
        }
        if self.blob_spammer {
            parts.push("blob-spammer");
        }
        write!(f, "{}", parts.join("+"))
    }
}

// ---------------------------------------------------------------------------
// CL gossip messages
// ---------------------------------------------------------------------------

/// Simplified beacon block proposal published at t=0.
///
/// In this simplified first-steps model there is no builder bid: the proposer
/// (which is also the builder) commits directly to the blobs it received over EL
/// networking. `blob_kzg_commitments` embed those blobs' announced hashes (see
/// [`commitment_for_blob_hash`]), so the proposal names exactly which EL blobs
/// the block includes and validators can match them against their own EL pool.
/// Other fields (attestations, slashings, …) are omitted for simplicity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedBeaconBlock {
    pub slot: u64,
    pub proposer_index: u64,
    /// One dummy KZG commitment (48 B) per included blob, hash-bearing.
    pub blob_kzg_commitments: Vec<Vec<u8>>,
    /// Dummy proposer BLS signature (96 bytes).
    pub signature: Vec<u8>,
}

/// Signed execution payload envelope (payload reveal) broadcast at t=4-6s.
///
/// It does not carry blob commitments: those were already announced in the t=0
/// proposal (`SignedBeaconBlock`), and since the proposer is also the builder,
/// the network already has them by the time the payload is revealed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedExecutionPayloadEnvelope {
    pub slot: u64,
    pub builder_index: u64,
    /// Dummy state root (32 bytes).
    pub state_root: [u8; 32],
    /// Dummy BLS signature (96 bytes).
    pub builder_signature: Vec<u8>,
    /// Execution-block body (dummy bytes). Only its length is meaningful — it
    /// models the on-wire cost of revealing the execution payload. Sized by
    /// `[sim].exec_payload_size_kib` (default [`EXEC_PAYLOAD_SIZE`]).
    pub payload: Vec<u8>,
}

/// A single blob sidecar accompanying the payload envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobSidecar {
    pub blob_index: u64,
    pub slot: u64,
    /// Dummy KZG commitment (48 bytes).
    pub kzg_commitment: Vec<u8>,
    /// Dummy KZG proof (48 bytes).
    pub kzg_proof: Vec<u8>,
    /// Full blob data (128 KiB = 64 cells × 2 KiB).
    pub blob_data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// EL devp2p messages (real point-to-point TCP transport, RLP-encoded)
// ---------------------------------------------------------------------------
//
// These mirror the execution-layer blob propagation flow: the builder announces
// blob hashes (eth/71 `NewPooledTransactionHashes` style), non-builder CL peers
// independently choose per blob whether to pull custody cells (sampler behavior)
// or the full payload (provider behavior), and the holder serves both. Unlike the
// CL messages above (JSON over gossipsub), these are RLP-encoded and sent over the
// dedicated EL TCP layer in `el_net.rs`. Byte fields use `Bytes` so RLP encodes
// them as byte strings (a `Vec<u8>` would RLP-encode as a list of individual bytes).

/// Blob hash announcement (simulates `NewPooledTransactionHashes` in eth/71).
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct BlobHashAnnounce {
    pub slot: u64,
    /// Dummy blob hashes (32 bytes each).
    pub blob_hashes: Vec<Bytes>,
}

/// Sampler custody-cell request.
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct CustodyCellRequest {
    pub slot: u64,
    pub blob_hash: Bytes,
    /// Indices of the custody columns requested.
    pub column_indices: Vec<u64>,
}

/// A single custody cell (column index + dummy cell data).
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct CustodyCell {
    pub column: u64,
    pub data: Bytes,
}

/// Sampler custody-cell response.
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct CustodyCellResponse {
    pub slot: u64,
    pub blob_hash: Bytes,
    /// Dummy cell data for the requested columns.
    pub cells: Vec<CustodyCell>,
}

/// Provider full-payload request.
///
/// NOTE: the ~128 KiB single-frame response requires a Shadow build with the
/// `tcp_sendUserData` 65535-byte send cap removed (patched `kamilsa/shadow-arm`
/// image / shadow-arm fork). On unpatched Shadow, a partial non-blocking send
/// happens even with buffer space free, edge-triggered epoll users (tokio) never
/// get the promised EPOLLOUT edge, and the connection deadlocks.
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct FullPayloadRequest {
    pub slot: u64,
    pub blob_hash: Bytes,
}

/// Provider full-payload response.
#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct FullPayloadResponse {
    pub slot: u64,
    pub blob_hash: Bytes,
    /// Dummy full blob data (derived from the hash — see [`payload_for_blob_hash`]).
    pub payload_data: Bytes,
}

/// EL wire message, RLP-encoded as `[msg_id byte | rlp(body)]` (devp2p style).
#[derive(Debug, Clone)]
pub enum ElMessage {
    Announce(BlobHashAnnounce),
    CustodyRequest(CustodyCellRequest),
    CustodyResponse(CustodyCellResponse),
    FullPayloadRequest(FullPayloadRequest),
    FullPayloadResponse(FullPayloadResponse),
}

impl ElMessage {
    /// Per-variant message id, written as the first byte of the frame.
    fn id(&self) -> u8 {
        match self {
            ElMessage::Announce(_) => 0,
            ElMessage::CustodyRequest(_) => 1,
            ElMessage::CustodyResponse(_) => 2,
            ElMessage::FullPayloadRequest(_) => 3,
            ElMessage::FullPayloadResponse(_) => 4,
        }
    }

    /// Encode as a message-id byte followed by the RLP-encoded body.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![self.id()];
        let body = match self {
            ElMessage::Announce(m) => alloy_rlp::encode(m),
            ElMessage::CustodyRequest(m) => alloy_rlp::encode(m),
            ElMessage::CustodyResponse(m) => alloy_rlp::encode(m),
            ElMessage::FullPayloadRequest(m) => alloy_rlp::encode(m),
            ElMessage::FullPayloadResponse(m) => alloy_rlp::encode(m),
        };
        out.extend_from_slice(&body);
        out
    }

    /// Decode a frame previously produced by [`ElMessage::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, alloy_rlp::Error> {
        let (id, mut rest) = bytes
            .split_first()
            .ok_or(alloy_rlp::Error::Custom("empty EL message"))?;
        let buf = &mut rest;
        let msg = match id {
            0 => ElMessage::Announce(BlobHashAnnounce::decode(buf)?),
            1 => ElMessage::CustodyRequest(CustodyCellRequest::decode(buf)?),
            2 => ElMessage::CustodyResponse(CustodyCellResponse::decode(buf)?),
            3 => ElMessage::FullPayloadRequest(FullPayloadRequest::decode(buf)?),
            4 => ElMessage::FullPayloadResponse(FullPayloadResponse::decode(buf)?),
            _ => return Err(alloy_rlp::Error::Custom("unknown EL message id")),
        };
        Ok(msg)
    }
}

// ---------------------------------------------------------------------------
// Wrapper enums for protocol messages
// ---------------------------------------------------------------------------

/// Gossipsub message wrapper — serialised to JSON before publishing.
///
/// CL-only: blob-hash announcements and the custody/full-payload request flow now
/// travel over the EL TCP layer (see [`ElMessage`]), not gossipsub.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    BeaconBlock(SignedBeaconBlock),
    Envelope(SignedExecutionPayloadEnvelope),
    Sidecar(BlobSidecar),
}

// ---------------------------------------------------------------------------
// Dummy data helpers
// ---------------------------------------------------------------------------

/// Total number of custody columns in the simulation (simplified PeerDAS param).
pub const NUM_CUSTODY_COLUMNS: u64 = 128;

/// Number of stable custody columns assigned to each non-builder CL node.
pub const CUSTODY_SUBSET_SIZE: usize = 4;

/// Deterministically pick a node's [`CUSTODY_SUBSET_SIZE`] custody columns from its
/// seed. A CL client subscribes only to these columns' subnets and only fetches
/// their cells (a supernode — deferred — would custody all [`NUM_CUSTODY_COLUMNS`]).
pub fn custody_columns_for_seed(seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed ^ 0xC057_0DA5_C011_5EED);
    let mut cols: HashSet<u64> = HashSet::new();
    while cols.len() < CUSTODY_SUBSET_SIZE {
        cols.insert(rng.gen_range(0..NUM_CUSTODY_COLUMNS));
    }
    cols.into_iter().collect()
}

/// Size of a single cell (column) in bytes. PeerDAS cell = 64 field elements ×
/// 32 B = 2 KiB.
pub const BYTES_PER_CELL: usize = 2 * 1024;

/// Number of cells in an (un-extended) blob: 64 cells × 2 KiB = 128 KiB.
pub const CELLS_PER_BLOB: usize = 64;

/// Full blob size in bytes (128 KiB). Reed-Solomon extension doubles this to
/// `NUM_CUSTODY_COLUMNS` (128) cells = 256 KiB across the extended column set.
pub const BLOB_SIZE: usize = BYTES_PER_CELL * CELLS_PER_BLOB;

/// Default number of blobs a blob-spammer originates per slot (its spam rate).
pub const BLOBS_PER_SLOT: usize = 6;

/// Default size (bytes) of the execution-payload body carried in the
/// [`SignedExecutionPayloadEnvelope`] a builder reveals each slot. 128 KiB is a
/// realistic mainnet execution-block body; tune via `[sim].exec_payload_size_kib`.
pub const EXEC_PAYLOAD_SIZE: usize = 128 * 1024;

/// Maximum blobs a builder includes in one block. Blobs beyond this stay pooled
/// for a later slot; the cap is what makes cross-slot inclusion tracking matter.
/// Under blocks-in-blobs (EIP-8142) the payload-blobs share this budget with the
/// EL blobs (payload-blobs come first).
pub const MAX_BLOBS_PER_BLOCK: usize = 6;

/// Usable bytes a single blob can carry for EIP-8142 payload encoding: 4096 field
/// elements × 31 usable bytes = 126,976 bytes. Used to size how many payload-blobs
/// an execution payload of a given size occupies.
pub const USABLE_BYTES_PER_BLOB: usize = 126_976;

/// Number of payload-blobs (EIP-8142) an execution payload of `exec_payload_size`
/// bytes occupies: `ceil(size / USABLE_BYTES_PER_BLOB)`. Zero for an empty payload.
/// The block's first `payload_blob_count` commitments are the payload-blobs.
pub fn payload_blob_count(exec_payload_size: usize) -> usize {
    exec_payload_size.div_ceil(USABLE_BYTES_PER_BLOB)
}

impl SignedBeaconBlock {
    /// A proposal committing to the given (hash-bearing) blob KZG commitments —
    /// built by the proposer from the blobs it pooled over EL networking.
    pub fn with_commitments(
        slot: u64,
        proposer_index: u64,
        blob_kzg_commitments: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            slot,
            proposer_index,
            blob_kzg_commitments,
            signature: vec![0xEE; 96],
        }
    }
}

impl SignedExecutionPayloadEnvelope {
    /// The payload-reveal envelope for a slot's block, carrying a `payload_size`-byte
    /// execution-block body. Blob commitments live in the t=0 proposal, not here.
    pub fn new(slot: u64, builder_index: u64, payload_size: usize) -> Self {
        Self {
            slot,
            builder_index,
            state_root: [0xBB; 32],
            builder_signature: vec![0xDD; 96],
            payload: vec![0xEE; payload_size],
        }
    }
}

/// Fill `n` bytes with random data drawn from `rng`. Used to give each blob
/// distinct, random payload content (seeded for reproducibility per node).
pub fn random_bytes(rng: &mut impl RngCore, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    rng.fill_bytes(&mut v);
    v
}

/// The full [`BLOB_SIZE`] payload for an announced blob hash, generated
/// deterministically from the hash. Every holder serves identical bytes for the
/// same blob, statelessly.
pub fn payload_for_blob_hash(blob_hash: &[u8]) -> Vec<u8> {
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    let mut seed = [0u8; 8];
    let n = blob_hash.len().min(8);
    seed[..n].copy_from_slice(&blob_hash[..n]);
    let mut rng = StdRng::seed_from_u64(u64::from_le_bytes(seed));
    random_bytes(&mut rng, BLOB_SIZE)
}

impl BlobSidecar {
    /// Build a sidecar carrying a full 128 KiB blob of random data. The KZG
    /// commitment/proof fields remain dummy placeholders.
    ///
    /// Test-only: in the simulation blobs originate at blob-spammers over EL
    /// networking; builders wrap pooled EL blobs instead of generating data.
    #[cfg(test)]
    pub fn random(slot: u64, index: u64, rng: &mut impl RngCore) -> Self {
        Self {
            blob_index: index,
            slot,
            kzg_commitment: {
                let mut c = vec![0xCC; 48];
                c[0] = index as u8;
                c
            },
            kzg_proof: vec![0xEE; 48],
            blob_data: random_bytes(rng, BLOB_SIZE),
        }
    }
}

impl BlobHashAnnounce {
    /// Build an announcement of `count` random 32-byte blob hashes. Random hashes
    /// keep every announced blob globally distinct.
    pub fn random(slot: u64, count: usize, rng: &mut impl RngCore) -> Self {
        Self {
            slot,
            blob_hashes: (0..count)
                .map(|_| Bytes::from(random_bytes(rng, 32)))
                .collect(),
        }
    }
}

// ===========================================================================
// EIP-7594 (PeerDAS) data columns + gossipsub 1.3 partial messages
// ===========================================================================
//
// These types model cell-level-delta propagation of data column sidecars over
// the gossipsub 1.3 partial-message protocol, mirroring Lighthouse. Under
// PeerDAS a block's blobs form a 2D matrix (rows = blobs, columns = 128 data
// columns). A *data column sidecar* is one vertical slice: one KZG cell per
// blob for a single column index. Partial messages let a publisher send each
// mesh peer only the cells it still lacks (a "delta") rather than the full
// column.
//
// As everywhere else in the sim, all cryptographic fields are dummy bytes.

/// A single KZG-encoded cell: 64 field elements × 32 B = 2 KiB (`BYTES_PER_CELL`).
pub type Cell = Vec<u8>;

/// Dummy KZG commitment / proof size in bytes (48 B on mainnet).
pub const KZG_ELEMENT_SIZE: usize = 48;

/// Number of gossipsub subnets for data column sidecars (one column per subnet
/// under Fulu). Column index maps to subnet via `index % DATA_COLUMN_SIDECAR_SUBNET_COUNT`.
pub const DATA_COLUMN_SIDECAR_SUBNET_COUNT: u64 = NUM_CUSTODY_COLUMNS;

/// Deterministic 32-byte block root for a slot. All nodes derive the same root
/// from the slot number so the columns they exchange assemble under one key.
/// (Real clients hash the block; the sim only needs agreement + uniqueness.)
pub fn block_root_for_slot(slot: u64) -> [u8; 32] {
    let mut root = [0u8; 32];
    root[..8].copy_from_slice(&slot.to_le_bytes());
    // Spread the slot across the root so distinct slots stay visually distinct
    // in logs; the exact transform is irrelevant (dummy crypto).
    for i in 8..32 {
        root[i] = (slot as u8).wrapping_mul(31).wrapping_add(i as u8);
    }
    root
}

/// Inverse of [`block_root_for_slot`]: recover the slot from the root's first
/// 8 bytes. Only valid for roots produced by this simulator (dummy crypto).
pub fn slot_for_block_root(root: &[u8; 32]) -> u64 {
    u64::from_le_bytes(root[..8].try_into().expect("8 bytes"))
}

/// Build a 48-byte dummy KZG commitment that embeds the blob's announced EL
/// hash (`0xC0 marker || 32-byte hash || padding`). Real clients compute the
/// commitment from blob data; here the embedded hash preserves the *identity*
/// correspondence — a block's commitments name exactly the EL blobs it
/// includes, and receivers can match their own EL blob pool against them.
pub fn commitment_for_blob_hash(hash: &[u8; 32]) -> Vec<u8> {
    let mut c = vec![0u8; KZG_ELEMENT_SIZE];
    c[0] = 0xC0;
    c[1..33].copy_from_slice(hash);
    c
}

/// Extract the blob hash embedded by [`commitment_for_blob_hash`].
pub fn blob_hash_from_commitment(commitment: &[u8]) -> Option<[u8; 32]> {
    if commitment.len() != KZG_ELEMENT_SIZE || commitment[0] != 0xC0 {
        return None;
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&commitment[1..33]);
    Some(hash)
}

/// A growable-length bitmap with one bit per blob slot (Lighthouse's
/// `CellBitmap = BitList<MaxBlobCommitmentsPerBlock>`). `len` bits are valid;
/// backing storage is `ceil(len / 8)` bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellBitmap {
    len: usize,
    bits: Vec<u8>,
}

impl CellBitmap {
    /// A bitmap of `len` bits, all cleared.
    pub fn with_len(len: usize) -> Self {
        Self {
            len,
            bits: vec![0u8; len.div_ceil(8)],
        }
    }

    /// A bitmap of `len` bits, all set (used to request every cell).
    pub fn all_set(len: usize) -> Self {
        let mut b = Self::with_len(len);
        for i in 0..len {
            b.set(i);
        }
        b
    }

    pub fn len(&self) -> usize {
        self.len
    }

    // Paired with `len()` to satisfy clippy; not otherwise used yet.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        (self.bits[idx / 8] >> (idx % 8)) & 1 == 1
    }

    pub fn set(&mut self, idx: usize) {
        if idx < self.len {
            self.bits[idx / 8] |= 1 << (idx % 8);
        }
    }

    /// Number of set bits.
    pub fn num_set_bits(&self) -> usize {
        (0..self.len).filter(|&i| self.get(i)).count()
    }

    /// True when every bit is set (the column is complete).
    pub fn is_full(&self) -> bool {
        self.num_set_bits() == self.len
    }

    /// Bitwise union (`self ∪ other`). Bitmaps must share the same length.
    pub fn union(&self, other: &Self) -> Self {
        let mut out = self.clone();
        for i in 0..self.len.min(other.len) {
            if other.get(i) {
                out.set(i);
            }
        }
        out
    }

    /// Set difference (`self \ other`): bits set here but not in `other`.
    pub fn difference(&self, other: &Self) -> Self {
        let mut out = Self::with_len(self.len);
        for i in 0..self.len {
            if self.get(i) && !other.get(i) {
                out.set(i);
            }
        }
        out
    }

    /// True when every set bit of `self` is also set in `other`.
    pub fn is_subset(&self, other: &Self) -> bool {
        (0..self.len).all(|i| !self.get(i) || other.get(i))
    }
}

/// The header carried once per block: the pieces needed to promote a completed
/// set of cells into a full `DataColumnSidecar`. Dummy crypto.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialDataColumnHeader {
    /// One dummy KZG commitment (48 B) per blob.
    pub kzg_commitments: Vec<Vec<u8>>,
    /// Dummy signed block header bytes.
    pub signed_block_header: Vec<u8>,
    /// Dummy KZG-commitments inclusion proof.
    pub kzg_commitments_inclusion_proof: Vec<u8>,
}

impl PartialDataColumnHeader {
    /// Build a header carrying the block's actual commitments (hash-bearing,
    /// from [`commitment_for_blob_hash`]); signature/proof stay dummy.
    pub fn from_commitments(kzg_commitments: Vec<Vec<u8>>) -> Self {
        Self {
            kzg_commitments,
            signed_block_header: vec![0xEE; 96 + 8],
            kzg_commitments_inclusion_proof: vec![0xAB; 32 * 4],
        }
    }
}

/// A full data column sidecar (all blobs' cells for one column index).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataColumnSidecar {
    pub index: u64,
    /// One cell (2 KiB) per blob.
    pub column: Vec<Cell>,
    /// One dummy KZG commitment (48 B) per blob.
    pub kzg_commitments: Vec<Vec<u8>>,
    /// One dummy cell KZG proof (48 B) per blob.
    pub kzg_proofs: Vec<Vec<u8>>,
    pub signed_block_header: Vec<u8>,
    pub kzg_commitments_inclusion_proof: Vec<u8>,
}

/// A sparse, partial data column sidecar: only the cells currently held, with a
/// bitmap marking which blob positions are present. Optionally carries the
/// header (only the first message a peer receives for a block includes it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialDataColumnSidecar {
    /// One bit per blob slot: set when that blob's cell is present below.
    pub cells_present_bitmap: CellBitmap,
    /// The present cells, in ascending blob-index order (sparse storage).
    pub column: Vec<Cell>,
    /// The present cells' dummy proofs, parallel to `column`.
    pub kzg_proofs: Vec<Vec<u8>>,
    /// Header, sent only in the first (header-only) message for a block.
    pub header: Option<PartialDataColumnHeader>,
}

impl PartialDataColumnSidecar {
    /// An empty sidecar (no cells) over `num_blobs` slots, optionally carrying a
    /// header. Used for the Phase-1 header-only message.
    pub fn empty(num_blobs: usize, header: Option<PartialDataColumnHeader>) -> Self {
        Self {
            cells_present_bitmap: CellBitmap::with_len(num_blobs),
            column: vec![],
            kzg_proofs: vec![],
            header,
        }
    }

    /// Number of present cells.
    pub fn num_present(&self) -> usize {
        self.cells_present_bitmap.num_set_bits()
    }

    /// True when a cell is present for every blob slot.
    pub fn is_complete(&self) -> bool {
        self.cells_present_bitmap.is_full()
    }

    /// Fetch the cell + proof at blob index `idx`, translating the bitmap
    /// position to the sparse storage position.
    pub fn get(&self, idx: usize) -> Option<(&Cell, &Vec<u8>)> {
        if !self.cells_present_bitmap.get(idx) {
            return None;
        }
        let storage_idx = (0..idx)
            .filter(|&i| self.cells_present_bitmap.get(i))
            .count();
        self.column
            .get(storage_idx)
            .zip(self.kzg_proofs.get(storage_idx))
    }

    /// Build a new sidecar containing only the present cells for which
    /// `keep(idx)` is true. The header is dropped (deltas never carry it).
    pub fn filter<F: Fn(usize) -> bool>(&self, keep: F) -> Self {
        let mut bitmap = CellBitmap::with_len(self.cells_present_bitmap.len());
        let mut column = Vec::new();
        let mut kzg_proofs = Vec::new();
        for idx in 0..self.cells_present_bitmap.len() {
            if self.cells_present_bitmap.get(idx) && keep(idx) {
                if let Some((cell, proof)) = self.get(idx) {
                    bitmap.set(idx);
                    column.push(cell.clone());
                    kzg_proofs.push(proof.clone());
                }
            }
        }
        Self {
            cells_present_bitmap: bitmap,
            column,
            kzg_proofs,
            header: None,
        }
    }

    /// Merge another sidecar's cells into this one (bitmap union). On overlap
    /// the existing cell is kept. Returns the merged sidecar.
    pub fn merge(&self, other: &Self) -> Self {
        let len = self.cells_present_bitmap.len();
        let new_bitmap = self.cells_present_bitmap.union(&other.cells_present_bitmap);
        let mut column = Vec::with_capacity(new_bitmap.num_set_bits());
        let mut kzg_proofs = Vec::with_capacity(new_bitmap.num_set_bits());
        for idx in 0..len {
            if let Some((cell, proof)) = self.get(idx).or_else(|| other.get(idx)) {
                column.push(cell.clone());
                kzg_proofs.push(proof.clone());
            }
        }
        Self {
            cells_present_bitmap: new_bitmap,
            column,
            kzg_proofs,
            header: self.header.clone().or_else(|| other.header.clone()),
        }
    }

    /// Encode for the wire (gossipsub `Event::Partial.message`) using a compact
    /// binary codec.
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialize partial sidecar")
    }

    /// Decode from wire bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

/// A partial data column tied to a specific block + column index. This is what
/// `decode_partial` produces and what the assembler accumulates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialDataColumn {
    pub block_root: [u8; 32],
    pub index: u64,
    pub sidecar: PartialDataColumnSidecar,
}

impl PartialDataColumn {
    /// Promote to a full `DataColumnSidecar` when every cell is present, using
    /// the block's header for the commitment/signature fields.
    pub fn try_clone_full(&self, header: &PartialDataColumnHeader) -> Option<DataColumnSidecar> {
        if !self.sidecar.is_complete() {
            return None;
        }
        Some(DataColumnSidecar {
            index: self.index,
            column: self.sidecar.column.clone(),
            kzg_commitments: header.kzg_commitments.clone(),
            kzg_proofs: self.sidecar.kzg_proofs.clone(),
            signed_block_header: header.signed_block_header.clone(),
            kzg_commitments_inclusion_proof: header.kzg_commitments_inclusion_proof.clone(),
        })
    }
}

/// Per-peer metadata exchanged inside the partial protocol: which cells the peer
/// has (`available`) and which it wants (`requests`). Merged (unioned) across
/// rounds so a publisher's view of a peer only grows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialDataColumnPartsMetadata {
    pub available: CellBitmap,
    pub requests: CellBitmap,
}

impl PartialDataColumnPartsMetadata {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialize parts metadata")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

/// Mock of Lighthouse's `blobs_to_data_column_sidecars`: reshape a block's blobs
/// into `NUM_CUSTODY_COLUMNS` data column sidecars, one cell per blob per column.
///
/// Real clients KZG-extend each blob to `NUM_CUSTODY_COLUMNS` cells; here we
/// derive each `(column, blob)` cell deterministically from the blob bytes so
/// results stay reproducible. Only cell sizes (2 KiB) and determinism matter for
/// bandwidth accounting — there is no real KZG.
pub fn blobs_to_data_column_sidecars(
    blobs: &[BlobSidecar],
    header: &PartialDataColumnHeader,
) -> Vec<DataColumnSidecar> {
    let num_blobs = blobs.len();
    (0..NUM_CUSTODY_COLUMNS)
        .map(|col| {
            let column: Vec<Cell> = blobs
                .iter()
                .map(|blob| derive_cell(&blob.blob_data, col))
                .collect();
            DataColumnSidecar {
                index: col,
                column,
                kzg_commitments: header.kzg_commitments.clone(),
                kzg_proofs: (0..num_blobs)
                    .map(|_| vec![0xEE; KZG_ELEMENT_SIZE])
                    .collect(),
                signed_block_header: header.signed_block_header.clone(),
                kzg_commitments_inclusion_proof: header.kzg_commitments_inclusion_proof.clone(),
            }
        })
        .collect()
}

/// Derive column `col`'s 2 KiB cell for a single blob. For the original 64
/// columns this slices the corresponding blob cell; for the extension columns it
/// reuses a cell cyclically with a deterministic byte rotation so cells stay
/// distinct. Dummy stand-in for KZG cell extension.
///
/// Public so the state machine can derive individual custody columns' cells from
/// locally-pooled EL blobs without building all 128 columns.
pub fn derive_cell(blob_data: &[u8], col: u64) -> Cell {
    let base = (col as usize % CELLS_PER_BLOB) * BYTES_PER_CELL;
    let mut cell = vec![0u8; BYTES_PER_CELL];
    let end = (base + BYTES_PER_CELL).min(blob_data.len());
    if base < blob_data.len() {
        cell[..end - base].copy_from_slice(&blob_data[base..end]);
    }
    // Extension columns (>= CELLS_PER_BLOB) get a deterministic rotation so the
    // extended cells differ from the originals.
    if col as usize >= CELLS_PER_BLOB {
        let rot = (col as u8).wrapping_mul(17);
        for b in cell.iter_mut() {
            *b = b.wrapping_add(rot);
        }
    }
    cell
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn blob_data_is_seeded_reproducible_and_distinct() {
        // Same RNG seed → identical blob bytes (reproducible per producer).
        let a = BlobSidecar::random(0, 0, &mut StdRng::seed_from_u64(123));
        let b = BlobSidecar::random(0, 0, &mut StdRng::seed_from_u64(123));
        assert_eq!(a.blob_data, b.blob_data);
        assert_eq!(a.blob_data.len(), BLOB_SIZE);

        // Different seed → different blob bytes (distinct across producers; this is
        // what a per-node `node_id` mixed into the seed achieves for spammers).
        let c = BlobSidecar::random(0, 0, &mut StdRng::seed_from_u64(456));
        assert_ne!(a.blob_data, c.blob_data);

        // Consecutive blobs drawn from the same RNG differ from one another.
        let mut rng = StdRng::seed_from_u64(123);
        let x = BlobSidecar::random(0, 0, &mut rng);
        let y = BlobSidecar::random(0, 1, &mut rng);
        assert_ne!(x.blob_data, y.blob_data);
    }

    #[test]
    #[should_panic(expected = "EL-only")]
    fn blob_spammer_cannot_combine_with_cl_roles() {
        let _ = NodeRoles::from_roles(&[Role::BlobSpammer, Role::Builder]);
    }

    #[test]
    fn sampler_provider_are_not_cli_roles() {
        assert!("validator".parse::<Role>().is_ok());
        assert!("sampler".parse::<Role>().is_err());
        assert!("provider".parse::<Role>().is_err());
    }

    #[test]
    fn block_root_slot_roundtrip() {
        for slot in [0u64, 1, 7, 12345, u64::MAX] {
            assert_eq!(slot_for_block_root(&block_root_for_slot(slot)), slot);
        }
    }

    #[test]
    fn commitment_blob_hash_roundtrip() {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&random_bytes(&mut StdRng::seed_from_u64(9), 32));
        let commitment = commitment_for_blob_hash(&hash);
        assert_eq!(commitment.len(), KZG_ELEMENT_SIZE);
        assert_eq!(blob_hash_from_commitment(&commitment), Some(hash));
        // Non-hash-bearing commitments don't decode.
        assert_eq!(
            blob_hash_from_commitment(&vec![0xCC; KZG_ELEMENT_SIZE]),
            None
        );
    }
}
