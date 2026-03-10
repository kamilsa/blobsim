# blob-sim Development Guide for AI Agents

This guide provides instructions for AI agents working on the blob-sim codebase — a Shadow-compatible Ethereum blob propagation simulator.

## Project Overview

blob-sim simulates EIP-7732 (ePBS) and EIP-8070 (Sparse Blobpool) blob propagation using real `libp2p` (QUIC transport) with mocked cryptographic payloads. The binary runs inside the [Shadow](https://shadow.github.io/) discrete-event network simulator.

### Key Constraints

- **No real cryptography** — KZG commitments, proofs, and BLS signatures are dummy `Vec<u8>`. Never add actual crypto crates.
- **Deterministic** — All randomness flows through `rand::rngs::StdRng::seed_from_u64`. Never use `thread_rng()` or OS entropy.
- **Shadow-compatible** — Use standard `tokio::time::sleep` / `Instant` so Shadow's libc interposition controls the simulated clock. Do not use `std::time::SystemTime::now()` or wall-clock time.
- **Decoupled architecture** — `network.rs` builds the swarm; `state_machine.rs` drives it. Networking must never contain slot logic, and the state machine must never construct swarm internals.

## Architecture

```
src/
├── main.rs           # CLI (clap), tracing init, swarm bootstrap, persona dispatch
├── network.rs        # libp2p swarm: QUIC transport, Gossipsub, Request-Response
├── state_machine.rs  # 12-second slot ticker, persona-driven event handling
└── types.rs          # Mock Ethereum message types + dummy constructors
```

### Module Responsibilities

| Module | Owns | Does NOT own |
|---|---|---|
| `types.rs` | All message structs, `NodePersona` enum, constants, dummy constructors | Serialization format, network topics |
| `network.rs` | Swarm creation, transport, topic/protocol constants, subscribe/dial helpers | Slot timing, message generation |
| `state_machine.rs` | Slot loop, phase transitions, persona logic, event dispatch | Swarm configuration, transport details |
| `main.rs` | CLI parsing, tracing init, orchestration | Business logic |

### Gossipsub Topics

- `/cl/bids/1` — Builder bids
- `/cl/payload_envelope/1` — Signed execution payload envelopes
- `/cl/blob_sidecar/1` — Blob sidecars
- `/cl/ptc_attestation/1` — PTC attestation messages
- `/el/blob_hash/1` — Blob hash announcements (simulated devp2p eth/71)

### Request-Response Protocol

- Protocol: `/sim/devp2p/1` (JSON codec)
- Request types: `CustodyCellRequest`, `FullPayloadRequest`
- Response types: `CustodyCellResponse`, `FullPayloadResponse`

### Node Personas

| Persona | Slot Behaviour |
|---|---|
| `Builder` | t=0s: publish bid + blob hashes. t=4-6s: publish envelope + sidecars. Responds to data requests. |
| `Sampler` | Reacts to `BlobHashAnnounce`: sends `CustodyCellRequest` (custody subset + 1 random extra) |
| `Provider` | Reacts to `BlobHashAnnounce`: sends `FullPayloadRequest` |
| `PtcMember` | t=8s: checks received data, publishes `PayloadAttestationMessage` |

## Development Workflow

### Essential Commands

```bash
# Type-check (fast feedback)
cargo check

# Full build
cargo build

# Release build (for Shadow)
cargo build --release

# Format
cargo fmt

# Lint
cargo clippy -- -D warnings

# Run with CLI
cargo run -- --persona builder --port 9000 --seed 1 --slots 2
```

### Adding a New Message Type

1. Define the struct in `types.rs` with `#[derive(Debug, Clone, Serialize, Deserialize)]`
2. Add a `dummy()` constructor if the type needs generation
3. Add it to the appropriate wrapper enum (`GossipMessage`, `SimRequest`, or `SimResponse`)
4. If it's a gossip message, add a topic constant in `network.rs` and subscribe in `all_topics()`
5. Handle it in `state_machine.rs`'s `handle_gossip_message()` or `handle_incoming_request()`

### Adding a New Persona

1. Add a variant to `NodePersona` in `types.rs`
2. Update `FromStr`, `Display` impls
3. Add persona-specific logic in `state_machine.rs`'s `run_node()` (slot phases) and `handle_gossip_message()` (reactions)
4. Update `main.rs` CLI help text if needed

### Serde Limitations

serde only supports `Serialize`/`Deserialize` for `[u8; N]` where N ≤ 32. For larger fixed-size crypto fields, use `Vec<u8>` instead. This is why signatures (96 bytes) and KZG commitments (48 bytes) are `Vec<u8>`.

## Common Pitfalls

1. **Don't use `SystemTime` or `Utc::now()`** — Shadow controls time via `tokio::time`. Using wall-clock time breaks determinism.
2. **Don't use `thread_rng()`** — Always use the seeded `StdRng` passed through the call chain.
3. **Don't block the async runtime** — Use `tokio::task::spawn_blocking` for any CPU-heavy work.
4. **Gossip publish may fail early** — `PublishError::InsufficientPeers` is expected when the mesh hasn't formed yet. These are logged as warnings, not panics.
5. **Don't add real crypto crates** — The entire point is to avoid KZG/BLS overhead in Shadow. If you need to vary dummy data, change the byte patterns.

## 12-Second Slot Phases

```
t=0s    → Bid phase (Builder publishes)
t=0-4s  → Drain events (all nodes process incoming messages)
t=4s    → Attestation window (mocked)
t=4-6s  → Payload release (Builder publishes envelope + sidecars)
t=6-8s  → Drain events
t=8s    → PTC vote phase (PTC members publish attestations)
t=8-12s → Drain events
t=12s   → Slot boundary → next slot
```

The `drain_events_until()` function uses `tokio::select!` to multiplex swarm events with phase deadlines.

## Dependencies

| Crate | Version | Why this version |
|---|---|---|
| `libp2p` | `0.54` | Latest stable with QUIC + gossipsub + req-res; compatible with Shadow |
| `tokio` | `1` | Shadow intercepts its libc time calls for clock simulation |
| `tracing` | `0.1` | Structured logs parseable by Shadow's log infrastructure |
| `clap` | `4` | CLI with derive macros |
| `serde` + `serde_json` | `1` | JSON codec for request-response protocol |
| `rand` | `0.8` | `StdRng::seed_from_u64` for determinism |
| `futures` | `0.3` | `StreamExt` for swarm event loop |

Do not upgrade `libp2p` without verifying Shadow compatibility (QUIC socket options, syscall support).
