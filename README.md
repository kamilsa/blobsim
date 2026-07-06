# blob-sim

A Shadow-compatible network simulator for Ethereum blob propagation under **EIP-7732 (ePBS)** and **EIP-8070 (Sparse Blobpool)** rule sets.

Built with real `libp2p` (QUIC transport) and mocked `devp2p` message flows — no actual cryptographic verification or EVM execution. Designed to run inside the [Shadow](https://shadow.github.io/) discrete-event network simulator.

## Architecture

```
src/
├── main.rs           # CLI entry point + swarm bootstrap
├── network.rs        # libp2p swarm, QUIC transport, Gossipsub + Req-Res
├── state_machine.rs  # 12-second slot ticker with persona-driven logic
└── types.rs          # Mock Ethereum message types (dummy crypto fields)
```

The networking layer (`network.rs`) is strictly decoupled from the state machine (`state_machine.rs`). The network module builds and configures the swarm; the state machine drives it via the event loop.

### Gossipsub Topics

| Topic | Layer | Message |
|---|---|---|
| `/cl/bids/1` | CL | `ExecutionPayloadBid` |
| `/cl/payload_envelope/1` | CL | `SignedExecutionPayloadEnvelope` |
| `/cl/blob_sidecar/1` | CL | `BlobSidecar` |
| `/cl/ptc_attestation/1` | CL | `PayloadAttestationMessage` |
| `/el/blob_hash/1` | EL | `BlobHashAnnounce` (simulated `eth/71`) |

### Request-Response Protocol

Protocol `/sim/devp2p/1` with JSON codec, carrying `CustodyCellRequest`/`FullPayloadRequest` and their responses.

## Node Roles

Each node is configured at startup with one or more roles:

| Role | Behaviour |
|---|---|
| **Proposer** | Publishes beacon block proposals |
| **Builder** | Releases payload envelope + blob sidecars, always requests full EL blobs |
| **Validator** | Non-builder CL node; for each announced EL blob independently chooses sampler behavior (85%) or provider behavior (15%) |
| **Blob Spammer** | EL-only load generator that originates blobs |

## 12-Second Slot Timeline

```
t=0s    Builder publishes bid + blob hash announce
t=4s    Standard attestation window (mocked)
t=4-6s  Builder releases payload envelope + blob sidecars
t=8s    PTC members vote on payload timeliness
t=12s   Slot boundary → next slot
```

## Usage

```bash
blob-sim --role <proposer|builder|validator|blob-spammer> \
         [--port <u16>] \
         [--el-port <u16>] \
         [--seed <u64>] \
         [--peer <multiaddr> ...] \
         [--el-peer <socket-addr> ...] \
         [--slots <u64>]
```

| Flag | Default | Description |
|---|---|---|
| `--role` | *(required)* | Node role, repeatable |
| `--port` | `0` (OS-assigned) | QUIC listen port |
| `--el-port` | `0` (OS-assigned) | EL TCP listen port |
| `--seed` | `42` | Deterministic RNG seed (keypair + random decisions) |
| `--peer` | *(none)* | Bootstrap peer multiaddrs (repeatable) |
| `--el-peer` | *(none)* | EL peer socket addresses (repeatable) |
| `--slots` | `10` | Number of 12-second slots to simulate |

### Local Smoke Test

```bash
# Terminal 1 — Proposer + builder
cargo run -- --role proposer --role builder --port 9000 --seed 1 --slots 2

# Terminal 2 — Validator
cargo run -- --role validator --port 9001 --seed 2 --slots 2 \
  --peer /ip4/127.0.0.1/udp/9000/quic-v1
```

### Shadow

Compile in release mode and reference the binary in your Shadow YAML config:

```bash
cargo build --release
# Binary at: target/release/blob-sim
```

## Design Constraints

- **No cryptography** — KZG commitments, proofs, and BLS signatures are fixed-size dummy byte vectors.
- **Deterministic** — Keypairs derived from `--seed`; all randomness via `StdRng::seed_from_u64`. No hardware entropy.
- **Shadow-friendly** — Uses standard `tokio::time::sleep` / `Instant` so Shadow's libc interposition can control the simulated clock.

## Dependencies

| Crate | Version | Purpose |
|---|---|---|
| `libp2p` | 0.54 | Gossipsub, Request-Response, QUIC transport |
| `tokio` | 1.x | Async runtime |
| `tracing` | 0.1 | Structured logging (Shadow log parsing) |
| `clap` | 4.x | CLI argument parsing |
| `serde` / `serde_json` | 1.x | Message serialization |
| `rand` | 0.8 | Deterministic RNG |
| `futures` | 0.3 | Stream combinators |
