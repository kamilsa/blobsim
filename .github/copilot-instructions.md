# blob-sim — Copilot Instructions

Shadow-compatible Ethereum blob propagation simulator (EIP-7732 / EIP-8070). Uses real `libp2p` with mocked cryptography, designed to run inside the [Shadow](https://shadow.github.io/) discrete-event network simulator.

## Commands

```bash
cargo check                        # Fast type-check
cargo build                        # Debug build
cargo build --release              # Release build (required for Shadow)
cargo fmt                          # Format
cargo clippy -- -D warnings        # Lint (warnings are errors)
```

There are no tests. The simulation itself is the validation harness — run with two terminals for a smoke test:

```bash
# Terminal 1 — Builder
cargo run -- --role builder --port 9000 --seed 1 --slots 2

# Terminal 2 — Sampler + PTC
cargo run -- --role sampler --role ptc --port 9001 --seed 2 --slots 2 \
  --peer /ip4/127.0.0.1/udp/9000/quic-v1
```

## Architecture

Four source modules with strict ownership boundaries:

| Module | Owns | Does NOT own |
|---|---|---|
| `types.rs` | Message structs, `Role` enum, `NodeRoles`, constants, `dummy()` constructors | Serialization format, network topics |
| `network.rs` | Swarm creation, QUIC transport, topic/protocol constants, subscribe/dial helpers | Slot timing, message generation |
| `state_machine.rs` | 12-second slot loop, phase transitions, role-driven event dispatch | Swarm configuration, transport details |
| `main.rs` | CLI parsing (`clap` derive), tracing init, orchestration | Business logic |
| `metrics.rs` | Bandwidth accounting (EL vs CL traffic split), per-slot/cumulative stats | Simulation logic |

**Key boundary rule:** `network.rs` must never contain slot logic; `state_machine.rs` must never construct swarm internals.

### 12-Second Slot Phases

```
t=0s    → Builder publishes bid + blob hash announce
t=0-4s  → Drain events (all nodes process incoming)
t=4-6s  → Builder releases payload envelope + blob sidecars
t=6-8s  → Drain events
t=8s    → PTC members publish attestations
t=8-12s → Drain events
t=12s   → Slot boundary → next slot
```

The `drain_events_until()` function uses `tokio::select!` to multiplex swarm events with phase deadlines.

### Gossipsub Topics & Request-Response

- Topics: `/cl/bids/1`, `/cl/payload_envelope/1`, `/cl/blob_sidecar/1`, `/cl/ptc_attestation/1`, `/el/blob_hash/1`
- Request-Response protocol: `/sim/devp2p/1` (JSON codec) with `CustodyCellRequest`/`FullPayloadRequest`

### Node Roles

Nodes can hold multiple roles. **Sampler and Provider are mutually exclusive** (enforced by panic in `NodeRoles::from_roles()`).

| Role | Behaviour |
|---|---|
| Builder | t=0s: publish bid + blob hashes. t=4-6s: publish envelope + sidecars. Responds to data requests. |
| Sampler | On `BlobHashAnnounce`: sends `CustodyCellRequest` (custody subset + 1 random extra) |
| Provider | On `BlobHashAnnounce`: sends `FullPayloadRequest` |
| PtcMember | t=8s: checks received data, publishes `PayloadAttestationMessage` |

## Key Conventions

### No Real Cryptography

KZG commitments, proofs, and BLS signatures are dummy `Vec<u8>` (not `[u8; N]` — serde only supports fixed arrays up to 32 bytes). Never add actual crypto crates; if you need to vary dummy data, change the byte patterns.

### Determinism

All randomness flows through `rand::rngs::StdRng::seed_from_u64`. The single `--seed` CLI param drives both keypair generation and builder/validator indices. Never use `thread_rng()` or OS entropy.

### Shadow Compatibility

Use `tokio::time::sleep` / `Instant` for all timing — Shadow intercepts libc time calls. Never use `std::time::SystemTime::now()` or `Utc::now()`. Do not upgrade `libp2p` without verifying Shadow compatibility (QUIC socket options, syscall support).

### Metrics

`metrics.rs` emits structured log lines with `target: "metrics"` for Shadow post-processing. It tracks EL vs CL traffic split by topic prefix and per-slot/cumulative byte counts.

### Adding a New Message Type

1. Define the struct in `types.rs` with `#[derive(Debug, Clone, Serialize, Deserialize)]`
2. Add a `dummy()` constructor if the type needs generation
3. Add it to the wrapper enum (`GossipMessage`, `SimRequest`, or `SimResponse`)
4. If gossip: add a topic constant in `network.rs` and subscribe in `all_topics()`
5. Handle in `state_machine.rs` via `handle_gossip_message()` or `handle_incoming_request()`

### Adding a New Role

1. Add variant to `Role` in `types.rs`
2. Update `FromStr`, `Display` impls, and `NodeRoles` struct
3. Add logic in `state_machine.rs`'s `run_node()` and `handle_gossip_message()`
