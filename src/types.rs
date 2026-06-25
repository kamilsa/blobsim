//! Mock Ethereum types for blob propagation simulation.
//!
//! All cryptographic fields (KZG commitments, BLS signatures, proofs) are replaced
//! with `Vec<u8>` dummy byte vectors to avoid CPU overhead inside Shadow and to
//! stay within serde's default array-size support.

use alloy_rlp::{Bytes, Decodable, RlpDecodable, RlpEncodable};
use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Node roles
// ---------------------------------------------------------------------------

/// Individual role a node can hold, configured via CLI `--role` flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Proposes beacon blocks containing the builder's bid at t=0.
    Proposer,
    /// Releases payloads + blobs at t=4-6s. Announces blob hashes at t=0.
    Builder,
    /// EIP-8070 sampler (85% of network): requests custody cells + 1 extra.
    Sampler,
    /// EIP-8070 provider (15% of network): requests the full payload matrix.
    Provider,
    /// PTC member: votes on payload timeliness at t=8s.
    PtcMember,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proposer => write!(f, "proposer"),
            Self::Builder => write!(f, "builder"),
            Self::Sampler => write!(f, "sampler"),
            Self::Provider => write!(f, "provider"),
            Self::PtcMember => write!(f, "ptc"),
        }
    }
}

impl std::str::FromStr for Role {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "proposer" => Ok(Self::Proposer),
            "builder" => Ok(Self::Builder),
            "sampler" => Ok(Self::Sampler),
            "provider" => Ok(Self::Provider),
            "ptc" | "ptc_member" | "ptcmember" => Ok(Self::PtcMember),
            other => Err(format!("unknown role: {other}")),
        }
    }
}

/// Set of roles a single node holds. A node may combine roles freely except
/// that **Sampler and Provider are mutually exclusive**.
#[derive(Debug, Clone)]
pub struct NodeRoles {
    pub proposer: bool,
    pub builder: bool,
    pub sampler: bool,
    pub provider: bool,
    pub ptc_member: bool,
}

impl NodeRoles {
    /// Build from a list of CLI-provided roles. Panics if both Sampler and
    /// Provider are requested.
    pub fn from_roles(roles: &[Role]) -> Self {
        let mut nr = Self {
            proposer: false,
            builder: false,
            sampler: false,
            provider: false,
            ptc_member: false,
        };
        for r in roles {
            match r {
                Role::Proposer => nr.proposer = true,
                Role::Builder => nr.builder = true,
                Role::Sampler => nr.sampler = true,
                Role::Provider => nr.provider = true,
                Role::PtcMember => nr.ptc_member = true,
            }
        }
        assert!(
            !(nr.sampler && nr.provider),
            "a node cannot be both sampler and provider"
        );
        nr
    }

    pub fn is_proposer(&self) -> bool {
        self.proposer
    }
    pub fn is_builder(&self) -> bool {
        self.builder
    }
    pub fn is_sampler(&self) -> bool {
        self.sampler
    }
    pub fn is_provider(&self) -> bool {
        self.provider
    }
    pub fn is_ptc_member(&self) -> bool {
        self.ptc_member
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
        if self.sampler {
            parts.push("sampler");
        }
        if self.provider {
            parts.push("provider");
        }
        if self.ptc_member {
            parts.push("ptc");
        }
        write!(f, "{}", parts.join("+"))
    }
}

// ---------------------------------------------------------------------------
// CL gossip messages
// ---------------------------------------------------------------------------

/// Builder bid (simplified EIP-7732 `ExecutionPayloadBid`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPayloadBid {
    pub slot: u64,
    pub builder_index: u64,
    /// Dummy KZG commitments root (32 bytes).
    pub blob_kzg_commitments_root: [u8; 32],
    pub bid_value_gwei: u64,
}

/// Builder bid wrapped with a BLS signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedExecutionPayloadBid {
    pub message: ExecutionPayloadBid,
    /// Dummy BLS signature (96 bytes).
    pub signature: Vec<u8>,
}

/// Simplified beacon block published by the proposer at t=0.
///
/// In ePBS (EIP-7732) the beacon block body includes the builder's signed bid
/// rather than an execution payload. Other fields (attestations, slashings, …)
/// are omitted for simulation simplicity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedBeaconBlock {
    pub slot: u64,
    pub proposer_index: u64,
    pub signed_execution_payload_bid: SignedExecutionPayloadBid,
    /// Dummy proposer BLS signature (96 bytes).
    pub signature: Vec<u8>,
}

/// Signed execution payload envelope broadcast at t=4-6s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedExecutionPayloadEnvelope {
    pub slot: u64,
    pub builder_index: u64,
    /// Dummy state root (32 bytes).
    pub state_root: [u8; 32],
    /// List of dummy KZG commitments (48 bytes each, as Vec<u8>).
    pub blob_kzg_commitments: Vec<Vec<u8>>,
    /// Dummy BLS signature (96 bytes).
    pub builder_signature: Vec<u8>,
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
    /// Small dummy blob data (not full 128 KiB — just enough to exercise networking).
    pub blob_data: Vec<u8>,
}

/// Payload status used in PTC attestations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayloadStatus {
    Present,
    Absent,
}

/// PTC attestation broadcast at t=8s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadAttestationMessage {
    pub slot: u64,
    pub validator_index: u64,
    pub payload_status: PayloadStatus,
    /// Dummy BLS signature (96 bytes).
    pub signature: Vec<u8>,
}

// ---------------------------------------------------------------------------
// EL devp2p messages (real point-to-point TCP transport, RLP-encoded)
// ---------------------------------------------------------------------------
//
// These mirror the execution-layer blob propagation flow: the builder announces
// blob hashes (eth/71 `NewPooledTransactionHashes` style), samplers pull custody
// cells and providers pull the full payload, and the builder serves both. Unlike
// the CL messages above (JSON over gossipsub), these are RLP-encoded and sent over
// the dedicated EL TCP layer in `el_net.rs`. Byte fields use `Bytes` so RLP encodes
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
    /// Dummy full blob data.
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
    PtcAttestation(PayloadAttestationMessage),
}

// ---------------------------------------------------------------------------
// Dummy data helpers
// ---------------------------------------------------------------------------

/// Total number of custody columns in the simulation (simplified PeerDAS param).
pub const NUM_CUSTODY_COLUMNS: u64 = 128;

/// Number of custody columns a sampler node is assigned.
pub const CUSTODY_SUBSET_SIZE: usize = 4;

/// Size of dummy blob data in bytes (small to save bandwidth in simulation).
pub const DUMMY_BLOB_SIZE: usize = 512;

/// Number of blobs per slot (simplified).
pub const BLOBS_PER_SLOT: usize = 6;

impl ExecutionPayloadBid {
    pub fn dummy(slot: u64, builder_index: u64) -> Self {
        Self {
            slot,
            builder_index,
            blob_kzg_commitments_root: [0xAA; 32],
            bid_value_gwei: 1_000_000,
        }
    }
}

impl SignedExecutionPayloadBid {
    pub fn dummy(slot: u64, builder_index: u64) -> Self {
        Self {
            message: ExecutionPayloadBid::dummy(slot, builder_index),
            signature: vec![0xDD; 96],
        }
    }
}

impl SignedBeaconBlock {
    pub fn dummy(slot: u64, proposer_index: u64, builder_index: u64) -> Self {
        Self {
            slot,
            proposer_index,
            signed_execution_payload_bid: SignedExecutionPayloadBid::dummy(slot, builder_index),
            signature: vec![0xEE; 96],
        }
    }
}

impl SignedExecutionPayloadEnvelope {
    pub fn dummy(slot: u64, builder_index: u64) -> Self {
        Self {
            slot,
            builder_index,
            state_root: [0xBB; 32],
            blob_kzg_commitments: (0..BLOBS_PER_SLOT as u8)
                .map(|i| {
                    let mut c = vec![0xCC; 48];
                    c[0] = i;
                    c
                })
                .collect(),
            builder_signature: vec![0xDD; 96],
        }
    }
}

impl BlobSidecar {
    pub fn dummy(slot: u64, index: u64) -> Self {
        Self {
            blob_index: index,
            slot,
            kzg_commitment: {
                let mut c = vec![0xCC; 48];
                c[0] = index as u8;
                c
            },
            kzg_proof: vec![0xEE; 48],
            blob_data: vec![0xFF; DUMMY_BLOB_SIZE],
        }
    }
}

impl BlobHashAnnounce {
    /// Build an announcement of `BLOBS_PER_SLOT` dummy 32-byte blob hashes.
    pub fn dummy(slot: u64) -> Self {
        Self {
            slot,
            blob_hashes: (0..BLOBS_PER_SLOT as u8)
                .map(|i| {
                    let mut h = vec![0x00u8; 32];
                    h[0] = i;
                    Bytes::from(h)
                })
                .collect(),
        }
    }
}
