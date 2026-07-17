# blobsim log streams

The simulator writes machine-parseable log lines to each host's
`shadow.data/hosts/<host>/blob-sim.1000.stdout`. The node identity is the **host
directory name** (never a field). Two tracing targets carry them:

- **`event`** — the structured `EVENT …` stream consumed by `notebooks/loaders.py`
  and the analysis notebook (`notebooks/analysis.ipynb`). Emitted from
  [`src/events.rs`](src/events.rs) via the `event!` macro.
- **`metrics`** — the coarse per-slot `METRIC` / `SUMMARY` counter lines from
  [`src/metrics.rs`](src/metrics.rs). Kept for the bandwidth fallback and quick
  greps.

## Timestamps

`t_ms` on every `EVENT` line is milliseconds on the **shared Shadow virtual clock**
(`SystemTime` → `CLOCK_REALTIME`), which every host reads from one simulated
source — so subtracting a per-run epoch yields comparable cross-node latencies.
The loader normalises to an epoch = median `slot_start` `t_ms` at slot 0, so
`t_ms − slot*12000` is the ms-into-slot (slots are 12 000 ms). Do **not** use
`tokio::time::Instant` (per-process `CLOCK_MONOTONIC`) for cross-node timing.

## EVENT line grammar

```
EVENT kind=<kind> t_ms=<u64> slot=<u64> <key>=<value> ...
```

Values are bare scalars, `true`/`false`, bracketed lists `[a,b,c]`, or the
sentinel `NA` for an unset optional. Fields are whitespace-separated. **Reserved
keys** `kind`, `t_ms`, `slot` must not be reused as field names — the `traffic`
event's message kind is therefore emitted as `mkind`, not `kind`.

## Event catalogue

| kind | fields (beyond `t_ms`, `slot`) | emitted by | feeds |
|---|---|---|---|
| `slot_start` | — | every node, slot top | loader epoch anchor |
| `slot_end` | `cl_peers`, `el_peers` | CL nodes, slot end | §1 (peer degree) |
| `blob_offered` | `blob=<hex>` | blob-spammer announce | §2 (blob creation time) |
| `block_published` | `n_blobs`, `payload_blobs`, `blobs=[<hex>,…]` | proposer | §2 |
| `arrival` | `atype=block` `{n_blobs}` · `atype=full_payload` `{blob}` · `atype=custody_cells` `{blob,cells}` | receiving node | §2 (blob p95), §3 (block CDF) |
| `readiness` | `is_cl_node`, `is_builder`, `is_zk_attester`, `eligible_envelope`, `eligible_custody`, `n_blobs`, `num_custody_columns`, `cells_held`, `cells_total`, `cl_peers`, `el_peers`, `block_t_ms\|NA`, `envelope_t_ms\|NA`, `custody_complete_t_ms\|NA` | every CL node at the **t=4 s attestation deadline** | §1 (custody count), §3 (cell possession) |
| `columns_seeded` | `columns`, `n_blobs` | builder, after seeding columns (= blob release) | §4 (release time) |
| `custody_complete` | — | a CL node the first time its **full custody set** assembles for a slot | §4 (completion time) |
| `blob_reconstruction_started` | `blob_index`, `generation`, `attempt`, `trigger`, `cells_held`, `complete_columns`, `delay_ms` | reconstruction-enabled non-builder supernode when a row becomes eligible | §4 reconstruction summary |
| `blob_reconstruction_completed` | `blob_index`, `generation`, `attempt`, `trigger`, `cells_added`, `columns_updated`, `outcome=reconstructed\|already-complete` | reconstruction-enabled non-builder supernode at the delayed deadline | §4 reconstruction summary |
| `blob_reconstruction_dropped` | `blob_index`, `generation`, `attempt`, `trigger`, `reason=assembly-evicted\|queue-capacity\|simulation-ended` | stale delayed job, bounded-scheduler overflow, or a job still pending at simulation end | §4 reconstruction summary |
| `traffic` | `layer=<cl\|el>`, `dir=<in\|out>`, `mkind=<msg kind>`, `bytes` | every `record_*` bandwidth call (see below) | §5 (per-message bandwidth) |

`traffic.mkind` values: `block`, `envelope`, `blob_sidecar`, `data_column`,
`partial_column`, `el_request`, `el_response`, `announce`. High-volume — one line
per message; the loader falls back to `METRIC` per-slot aggregates when absent.
`partial_column` is emitted in **both** directions: `dir=in` on each received
partial (`record_partial_received`) and `dir=out` on each publish/re-publish that
queues cells to peers (`record_partial_sent`) — the outbound side is the dominant
CL payload under EIP-8070 and must be present for the §5 send totals to be right.
Cells generated locally by reconstruction do not increment `partial_cells_received`.
Their re-publication uses the ordinary `partial_column dir=out` traffic path and is
therefore already included in outbound CL bandwidth.

## METRIC / SUMMARY lines (`metrics` target)

`METRIC slot=<n> roles=<r> el_bytes_sent=… el_bytes_received=… cl_bytes_sent=…
cl_bytes_received=… el_requests_sent=… … partial_cells_received=…
partial_columns_published=… partial_bytes_sent=… partial_columns_completed=…
partial_cells_pooled=…` — one per node per slot, per-slot byte/message counters.
`partial_bytes_sent` (body + metadata bytes queued to peers) is the outbound
counterpart of the inbound partial payload and is folded into `cl_bytes_sent`. `SUMMARY roles=… slots=… total_*_bytes_* …
avg_*_per_slot=…` — one per node at shutdown. The loader parses `METRIC` into
per-slot `slot_metric` bandwidth rows as the §5 fallback.

## Adding a field or event

Emit it via the `event!` macro at the relevant `src/*.rs` site, add its column to
the matching frame contract in `notebooks/loaders.py` (the `_*_COLS` lists), and
consume it in `notebooks/analysis.ipynb`. Never name a field `kind`/`t_ms`/`slot`.
