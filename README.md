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
| **Builder** | Releases payload envelope + blob sidecars, always requests full EL blobs. Under `--blocks-in-blobs` (EIP-8142) it also encodes the execution payload into payload-blobs and seeds them onto the column subnets |
| **Validator** | Non-builder CL node; for each announced EL blob independently chooses sampler behavior (85%) or provider behavior (15%) |
| **ZK Attester** | A validator that verifies via zkEVM proofs (EIP-8142): does **not** subscribe to the payload-envelope topic and instead receives only the payload-blob cells for its custody columns (partial payload — a non-supernode does not reconstruct the full payload). Combined with `--role validator` |
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
blob-sim --role <proposer|builder|validator|zk-attester|blob-spammer> \
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

## Running under Shadow (Docker)

`shadow-sim.py` is a convenient [`uv`](https://docs.astral.sh/uv/) launcher that
generates a geo-realistic network topology + a `shadow.yaml` and runs the whole
simulation with a single command. It supports two runners (set `[run].runner` in
`blobsim.toml`):

- **`docker`** (default) — builds `blob-sim` into a Shadow-capable image and runs
  Shadow in a container. Zero local setup beyond Docker.
- **`native`** — builds `blob-sim` with `cargo build --release` and runs a `shadow`
  binary from your `PATH`. Requires [Shadow](https://shadow.github.io/) installed
  locally (Linux).

```bash
# Build the image (if needed) and run the sim described by blobsim.toml
uv run shadow-sim.py

# Use a different config file (positional argument, defaults to blobsim.toml)
uv run shadow-sim.py my-config.toml

# Generate shadow.yaml + topology.gml only (no Docker) — inspect before running
uv run shadow-sim.py --dry-run

# Force a rebuild of the image (after changing Rust source), and wipe old results
uv run shadow-sim.py --rebuild --clean
```

**Requirements:** [`uv`](https://docs.astral.sh/uv/), plus — for the `docker` runner —
Docker (arm64 / Apple Silicon); the patched Shadow base image
(`kamilsa/shadow-arm:tcpfix`) is pulled automatically on first build. For the `native`
runner: a local [Shadow](https://shadow.github.io/) install (equally patched — see
below) and a Rust toolchain.

> **Why patched?** Stock Shadow's `tcp_sendUserData` caps every send at 65535 bytes
> even when the socket buffer has space, so a partial write happens on a non-full
> buffer. Edge-triggered epoll users (tokio) then wait for an EPOLLOUT edge that never
> fires, permanently deadlocking any connection that pushes a ≥64 KiB frame — like
> blob-sim's 128 KiB full-payload responses. The fix (in the local shadow fork)
> removes the cap so a short write means what POSIX implies: the buffer is full.

### Configuration

Everything is driven by [`blobsim.toml`](blobsim.toml) — edit it to change a run (pass a
different file as the positional argument; the launcher itself only takes `--dry-run`,
`--rebuild`, `--clean`). Key knobs:

- `[topology]` — `validators`, `zk_attesters` (how many validators are zk-attesters, EIP-8142), `blob_spammers`, and how many CL/EL peers each node dials.
- `[network]` — a **geo-realistic** model: each host is assigned a region, while
  `supernode_fraction` selects an exact seeded fraction of CL hosts that custody all
  128 columns. With effective partial columns and `enable_blob_reconstruction`,
  the launcher also enables mock reconstruction on selected non-builder supernodes.
  Set `supernode_bandwidth` and `non_supernode_bandwidth` independently; inter-host
  latency comes from an inter-region matrix + per-edge jitter.
- `[sim]` — `slots`, `seed`, `blobs_per_slot`, `exec_payload_size_kib`,
  `enable_partial_columns`, `disable_get_blobs`, `blocks_in_blobs` (EIP-8142;
  also propagate the payload as payload-blobs over the column subnets, implies
  `enable_partial_columns`), `enable_blob_reconstruction` (default `true`),
  `blob_reconstruction_delay_ms` (default 100), `blob_reconstruction_trigger`
  (`complete-columns` or `per-row`), `rust_log`.

The whole topology is deterministic in `[sim].seed`: same config → byte-identical
`shadow.yaml`/`topology.gml`.

### How it works

Every node runs the single `blob-sim` binary (baked into the image at
`/opt/blobsim/blob-sim` for the `docker` runner, or `target/release/blob-sim` for the
`native` runner) as its own Shadow host with a distinct IP. The launcher wires
both layers: CL peers over QUIC (`--peer`) and — unlike `run_network.sh` — **EL peers
over TCP** (`--el-peer`, always including ≥1 blob-spammer) so blobs originate at the
spammers and propagate through the sparse blobpool. Outputs land in `[output].dir`
(default `shadow-output/`):

```
shadow-output/
├── shadow.yaml        # generated Shadow config
├── topology.gml       # generated geo network graph
├── regions.json       # per-host region assignment (debug)
├── bandwidths.json    # per-host bandwidth tier (debug)
└── shadow.data/hosts/<host>/blob-sim.1000.stdout   # per-node logs
```

### Analysis

After each run the launcher prints a **summary** — blocks produced, per-slot block /
payload-envelope propagation reach, blob commitments, and EL/CL traffic totals — so you
can see at a glance whether a run actually exercised the network (and it flags common
issues like blocks that committed zero blobs). Re-print it for an existing run without
re-simulating:

```bash
uv run shadow-sim.py --summary-only
```

For the full analysis notebook (network overview, per-blob p95 latency, cell
possession at the attestation deadline, custody-fetch heatmaps, per-slot
bandwidth), run the observatory:

```bash
uv run shadow-sim.py --clean --serve       # run, render notebooks/analysis.ipynb, serve at :4321
uv run shadow-sim.py --serve-only          # serve previously rendered runs without simulating
```

It renders `notebooks/analysis.ipynb` against the run (via `scripts/render_notebooks.py`)
and serves the static Astro observatory (`site/`) at http://0.0.0.0:4321. The
`notebooks/loaders.py` parser turns the `EVENT`/`METRIC` logs into DataFrames — see
[`metrics.md`](metrics.md) for the log schema.

### Building the binary directly

If you just want the release binary (e.g. to reference from your own Shadow config):

```bash
cargo build --release   # → target/release/blob-sim
```

## Design Constraints

- **No cryptography** — KZG commitments, proofs, and BLS signatures are fixed-size dummy byte vectors.
- **Deterministic** — Keypairs derived from `--seed`; all randomness via `StdRng::seed_from_u64`. No hardware entropy.
- **Shadow-friendly** — Uses standard `tokio::time::sleep` / `Instant` so Shadow's libc interposition can control the simulated clock.

## Dependencies

| Crate | Version | Purpose |
|---|---|---|
| `libp2p` | 0.57 ([fork](https://github.com/kamilsa/rust-libp2p/tree/blobsim-patches)) | Gossipsub (incl. 1.3 partial messages), QUIC transport. Pinned to a fork: the `partial-messages` feature is not yet on crates.io, and blob-sim carries three gossipsub patches — see the `[dependencies]` comment in `Cargo.toml`. |
| `tokio` | 1.x | Async runtime |
| `tracing` | 0.1 | Structured logging (Shadow log parsing) |
| `clap` | 4.x | CLI argument parsing |
| `serde` / `serde_json` | 1.x | Message serialization |
| `rand` | 0.8 | Deterministic RNG |
| `futures` | 0.3 | Stream combinators |
