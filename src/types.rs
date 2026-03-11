//! Mock Ethereum types for blob propagation simulation.
//!
//! All cryptographic fields (KZG commitments, BLS signatures, proofs) are replaced
//! with `Vec<u8>` dummy byte vectors to avoid CPU overhead inside Shadow and to
//! stay within serde's default array-size support.

use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Node roles
// ---------------------------------------------------------------------------

/// Individual role a node can hold, configured via CLI `--role` flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Generates bids at t=0 and releases payloads + blobs at t=4-6s.
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
            builder: false,
            sampler: false,
            provider: false,
            ptc_member: false,
        };
        for r in roles {
            match r {
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

/// Builder bid broadcast at t=0s (simplified).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPayloadBid {
    pub slot: u64,
    pub builder_index: u64,
    /// Dummy KZG commitments root (32 bytes).
    pub blob_kzg_commitments_root: [u8; 32],
    pub bid_value_gwei: u64,
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
// EL devp2p messages (simulated via gossipsub + request-response)
// ---------------------------------------------------------------------------

/// Blob hash announcement (simulates `NewPooledTransactionHashes` in eth/71).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobHashAnnounce {
    pub slot: u64,
    /// Dummy blob hashes (32 bytes each).
    pub blob_hashes: Vec<[u8; 32]>,
}

/// Sampler custody-cell request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustodyCellRequest {
    pub slot: u64,
    pub blob_hash: [u8; 32],
    /// Indices of the custody columns requested.
    pub column_indices: Vec<u64>,
}

/// Sampler custody-cell response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustodyCellResponse {
    pub slot: u64,
    pub blob_hash: [u8; 32],
    /// Dummy cell data keyed by column index.
    pub cells: Vec<(u64, Vec<u8>)>,
}

/// Provider full-payload request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullPayloadRequest {
    pub slot: u64,
    pub blob_hash: [u8; 32],
}

/// Provider full-payload response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullPayloadResponse {
    pub slot: u64,
    pub blob_hash: [u8; 32],
    /// Dummy full blob data.
    pub payload_data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Wrapper enums for protocol messages
// ---------------------------------------------------------------------------

/// Gossipsub message wrapper — serialised to JSON before publishing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Bid(ExecutionPayloadBid),
    Envelope(SignedExecutionPayloadEnvelope),
    Sidecar(BlobSidecar),
    PtcAttestation(PayloadAttestationMessage),
    BlobHash(BlobHashAnnounce),
}

/// Request-response request wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SimRequest {
    CustodyCell(CustodyCellRequest),
    FullPayload(FullPayloadRequest),
}

/// Request-response response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SimResponse {
    CustodyCell(CustodyCellResponse),
    FullPayload(FullPayloadResponse),
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
    pub fn dummy(slot: u64) -> Self {
        Self {
            slot,
            blob_hashes: (0..BLOBS_PER_SLOT as u8)
                .map(|i| {
                    let mut h = [0x00; 32];
                    h[0] = i;
                    h
                })
                .collect(),
        }
    }
}
