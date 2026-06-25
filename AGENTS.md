# Agent Notes

## Source Of Truth

- Trust `src/main.rs`, `src/network.rs`, `src/el_net.rs`, and `src/state_machine.rs` over README-style docs when they disagree.
- `README.md`, `GEMINI.md`, and `.github/copilot-instructions.md` contain stale protocol/CLI references such as `--persona`, `/cl/bids/1`, `/el/blob_hash/1`, and `/sim/devp2p/1`; the current CLI uses repeatable `--role`, and EL traffic is raw TCP in `el_net.rs`.

## Commands

- Fast verification: `cargo check`.
- Formatting/linting: `cargo fmt --check` and `cargo clippy -- -D warnings`.
- Shadow binary: `cargo build --release` writes `target/release/blob-sim`.
- There are currently no Rust tests in `src`; use `cargo check` plus a local smoke run for behavior changes.
- Minimal CL+EL smoke run uses two processes; use `--slots 2` because slot 0 can start before peers connect:
  ```bash
  cargo run -- --role builder --port 9000 --el-port 9100 --seed 1 --slots 2
  cargo run -- --role sampler --role ptc --port 9001 --el-port 9101 --seed 2 --slots 2 --peer /ip4/127.0.0.1/udp/9000/quic-v1 --el-peer 127.0.0.1:9100
  ```
- `bash run_network.sh` builds and launches a local CL network, but its defaults start 102 nodes and clear `logs/`; pass small counts and a custom `--log-dir` when experimenting.
- `run_network.sh` currently wires only CL `--peer` flags, not EL `--el-port`/`--el-peer`, so it does not exercise EL request/response traffic without changes.

## Architecture Boundaries

- `main.rs` owns clap parsing, tracing setup, CL swarm creation, EL actor spawn, metrics creation, and `run_node(...)` orchestration.
- `network.rs` is consensus-layer only: libp2p QUIC + gossipsub, with topics `/cl/beacon_block/1`, `/cl/payload_envelope/1`, `/cl/blob_sidecar/1`, and `/cl/ptc_attestation/1`.
- `el_net.rs` is execution-layer only: a Tokio TCP actor using `[u32 big-endian length | msg_id byte | RLP body]` frames, no libp2p, no discovery, no RLPx.
- `state_machine.rs` owns all 12-second slot timing and role behavior; do not move slot logic into `network.rs` or swarm construction into `state_machine.rs`.
- `types.rs` defines roles and wire messages: CL gossip is JSON via `GossipMessage`; EL messages are RLP via `ElMessage::encode/decode`.
- `metrics.rs` emits `target: "metrics"` `METRIC` and `SUMMARY` log lines; update `create_notebook.py` before regenerating `Analysis.ipynb` if log fields or topic names change.

## Repo-Specific Constraints

- CLI roles are repeatable: `--role proposer|builder|sampler|provider|ptc`; sampler and provider are mutually exclusive, but other role combinations are allowed.
- Preserve deterministic simulation behavior: derive keypairs and random choices from the `--seed` path and `StdRng::seed_from_u64`; do not use `thread_rng()` or OS entropy.
- Preserve Shadow-compatible timing: phase deadlines use `tokio::time::Instant`/`sleep_until`; do not add wall-clock reads such as `SystemTime::now()` or `Utc::now()` to simulation logic.
- Cryptographic fields are dummy bytes by design. Keep BLS/KZG-sized fields as `Vec<u8>` where needed because serde only handles fixed arrays up to 32 bytes by default.
- `Cargo.toml` patches `quinn-udp` to `patch/quinn-udp`, a local fallback UDP implementation; keep this in mind when touching `libp2p`/QUIC dependency versions.
