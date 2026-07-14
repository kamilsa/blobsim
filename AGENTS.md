# Agent Notes

## Source Of Truth

- Trust `src/main.rs`, `src/network.rs`, `src/el_net.rs`, and `src/state_machine.rs` over README-style docs when they disagree.
- `README.md`, `GEMINI.md`, and `.github/copilot-instructions.md` contain stale protocol/CLI references such as `--persona`, `/cl/bids/1`, `/el/blob_hash/1`, and `/sim/devp2p/1`; the current CLI uses repeatable `--role`, and EL traffic is raw TCP in `el_net.rs`.

## Commands

- Fast verification: `cargo check`.
- Formatting/linting: `cargo fmt --check` and `cargo clippy -- -D warnings`.
- Shadow binary: `cargo build --release` writes `target/release/blob-sim`.
- There are a few `#[test]`s in `src/types.rs` (`cargo test`); use `cargo check` plus a local smoke run for behavior changes.
- Minimal CL+EL smoke run uses three processes — blobs originate at a blob-spammer, so a proposer/builder without an EL blob source produces blobless proposals. The block-producing node is a combined `--role proposer --role builder` (it proposes at t=0 committing to its pooled blobs). Use `--slots 3+`: slot 0 can start before peers connect, and the first proposal drains an empty pool:
  ```bash
  cargo run -- --role blob-spammer --el-port 9200 --seed 7 --node-id 1 --slots 3
  cargo run -- --role proposer --role builder --port 9000 --el-port 9100 --seed 1 --slots 3 --el-peer 127.0.0.1:9200
  cargo run -- --role validator --port 9001 --el-port 9101 --seed 2 --slots 3 --peer /ip4/127.0.0.1/udp/9000/quic-v1 --el-peer 127.0.0.1:9200
  ```
- `bash run_network.sh` builds and launches a local CL network, but its defaults start 102 nodes and clear `logs/`; pass small counts and a custom `--log-dir` when experimenting.
- `run_network.sh` currently wires only CL `--peer` flags, not EL `--el-port`/`--el-peer`, so it does not exercise EL request/response traffic without changes.

## Architecture Boundaries

- `main.rs` owns clap parsing, tracing setup, CL swarm creation, EL actor spawn, metrics creation, and `run_node(...)` orchestration.
- `network.rs` is consensus-layer only: libp2p QUIC + gossipsub, with topics `/cl/beacon_block/1`, `/cl/payload_envelope/1`, and `/cl/blob_sidecar/1`. With `--enable-partial-columns` it also joins per-subnet `/cl/data_column_sidecar/{subnet}/1` topics via `subscribe_partial` (gossipsub 1.3 partial messages).
- `partial.rs` implements the gossipsub 1.3 partial-message protocol for data column sidecars: the `Partial`/`Metadata` trait impls (`OutgoingPartialColumn`, `MaybeKnownMetadata`), the per-block header tracker, and the cell assembler. `libp2p` is pinned in `Cargo.toml` to the `blobsim-patches` branch of a [rust-libp2p fork](https://github.com/kamilsa/rust-libp2p) (upstream master rev `891bf049` + three gossipsub patches), because the `partial-messages` feature (gossipsub 0.50 / umbrella 0.57) is not yet on crates.io *and* the partial exchange needs local fixes: a longer partial-state TTL, no "stale data" skip on republish, and a byte count returned from `publish_partial`. See the comment above `[dependencies]` in `Cargo.toml`; to change a patch, rebase that branch and bump the `rev`.
- `el_net.rs` is execution-layer only: a Tokio TCP actor using `[u32 big-endian length | msg_id byte | RLP body]` frames, no libp2p, no discovery, no RLPx. The actor never awaits socket writes itself — each connection has its own reader and writer task (a slow peer must not block the actor or other peers).
- **Requires a patched Shadow for EL frames ≥ 64 KiB** (the 128 KiB `FullPayloadResponse`). Stock Shadow's `tcp_sendUserData` caps every send at 65535 bytes even when the send buffer has space (`src/main/host/descriptor/tcp.c`, `MIN(nBytes, 65535)`), so a partial write happens on a *non-full* buffer; edge-triggered epoll users (tokio/mio) treat the short write as "wait for EPOLLOUT", no writability edge ever fires, and the connection deadlocks — blocking sockets are unaffected. The fix (remove the cap: `remaining = MIN(nBytes, space)`) is committed in the shadow-arm fork (`~/dev/shadow`, commit `ae04b0890`) and published as `kamilsa/shadow-arm:tcpfix` (also `:latest`) on Docker Hub; Shadow's own tcp/epoll/send_recv test suite passes with it. Worth upstreaming to shadow/shadow. Do NOT reintroduce app-level chunking to work around this — it distorts the simulated wire behavior.
- `state_machine.rs` owns all 12-second slot timing and role behavior; do not move slot logic into `network.rs` or swarm construction into `state_machine.rs`.
- `types.rs` defines roles and wire messages: CL gossip is JSON via `GossipMessage`; EL messages are RLP via `ElMessage::encode/decode`.
- `metrics.rs` emits `target: "metrics"` `METRIC`/`SUMMARY` per-slot counter lines and per-message `traffic` events; `events.rs` emits the structured `target: "event"` `EVENT …` stream (see `metrics.md`). The analysis pipeline is `notebooks/loaders.py` (parses the logs into DataFrames) → `notebooks/analysis.ipynb` (§1–§5 Plotly) → `scripts/render_notebooks.py` (papermill + nbconvert → `site/rendered/`) → the Astro observatory (`site/`, served by `uv run shadow-sim.py --serve`). If you add or rename a log field, update `metrics.md`, the `_*_COLS` contracts in `loaders.py`, and the consuming notebook cell; never reuse the reserved `EVENT` keys `kind`/`t_ms`/`slot` as field names.

## Repo-Specific Constraints

- CLI roles are repeatable: `--role proposer|builder|validator|blob-spammer`. In the current model a proposer is also a builder (there is no bid).
- Blob pipeline: blobs originate at blob-spammers over EL networking and propagate via the sparse blobpool. For each announced blob, non-builder CL peers independently choose sampler behavior (85%, pull stable custody-set cells plus one random extra) or provider behavior (15%, pull the full payload). Builders never generate blob data — they always behave as providers on EL and pool the full blobs they receive (`ElBlobPool`, keyed by announced hash). At slot start a builder takes up to `MAX_BLOBS_PER_BLOCK` **not-yet-included** blobs from its pool for the block (overflow stays pooled for a later slot). The **proposal** (`SignedBeaconBlock`) published at t=0 carries `blob_kzg_commitments` that embed those announced hashes (`commitment_for_blob_hash`), so it names exactly the EL blobs the block includes. A validator that sees the proposal matches the commitments' hashes against its own EL pool (local `getBlobs`) and starts propagating custody columns, then evicts those included blobs from its pool. Inclusion tracking (`ElBlobPool.included`, a `INCLUDED_WINDOW_SLOTS`-slot window) prevents a blob from being re-pooled or re-included across slots. The t=4-6 payload-reveal envelope carries no commitments (they were already in the proposal) but does carry a configurable-size execution-block body (`SignedExecutionPayloadEnvelope.payload`, sized by `[sim].exec_payload_size_kib` → `--exec-payload-size`, default 128 KiB); a validator that missed the proposal instead triggers off a received partial column's header. (PTC has been removed for now.)
- `--enable-partial-columns` switches CL blob propagation to data column sidecars over gossipsub 1.3 partial messages (cell-level deltas); the baseline `/cl/blob_sidecar/1` full path is used otherwise. `engine_getBlobs` is modeled as a *local* read of the node's EL blob pool (full blobs the EL previously received over EL networking via announce → full-payload pulls) — never a network request from the CL side. `--disable-get-blobs` (only meaningful with partials) makes nodes ignore that pool and pull all custody cells from peers over CL.
- Preserve deterministic simulation behavior: derive keypairs and random choices from the `--seed` path and `StdRng::seed_from_u64`; do not use `thread_rng()` or OS entropy.
- Preserve Shadow-compatible timing: phase deadlines use `tokio::time::Instant`/`sleep_until`; do not add wall-clock reads such as `SystemTime::now()` or `Utc::now()` to simulation logic.
- Cryptographic fields are dummy bytes by design. Keep BLS/KZG-sized fields as `Vec<u8>` where needed because serde only handles fixed arrays up to 32 bytes by default.
- `Cargo.toml` patches `quinn-udp` to `patch/quinn-udp`, a local fallback UDP implementation; keep this in mind when touching `libp2p`/QUIC dependency versions.
