"""Load a blobsim run into tidy DataFrames for the analysis notebook.

The Rust sim writes one structured line per event to each host's stdout log:

    EVENT kind=<kind> t_ms=<u64> slot=<u64> <key>=<value> ...

(see ``src/events.rs``). ``load_run`` scans every host log, parses those lines,
normalises timestamps to a per-run epoch, joins the static topology artefacts
(``regions.json`` / ``bandwidths.json`` / ``topology.gml`` / ``blobsim.toml``),
and returns the frames the notebook sections consume:

    traffic, arrivals, columns, pool, slots, nodes, meta

Frames for events a given run did not emit come back empty but *with their
declared columns*, so downstream cells can filter without KeyErrors.
"""

from __future__ import annotations

import json
import os
import re
import tomllib
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_EVENT_RE = re.compile(r"EVENT\s+(.*)$")
_METRIC_RE = re.compile(r"METRIC\s+(.*)$")
# A key=value token: value is a bracketed list, or a run of non-space chars.
_KV_RE = re.compile(r"(\w+)=(\[[^\]]*\]|\S+)")

SLOT_MS = 12_000

# ---- declared column contracts -------------------------------------------------

_SLOTS_COLS = [
    "node", "event", "slot", "t_ms", "n_blobs", "payload_blobs", "blobs", "blob",
    "block_t_ms", "envelope_t_ms", "custody_complete_t_ms",
    "is_cl_node", "is_builder", "is_zk_attester",
    "eligible_envelope", "eligible_custody",
    "num_custody_columns", "cells_held", "cells_total",
    "cl_peers", "el_peers", "role_group", "region", "bandwidth",
]
_ARRIVALS_COLS = [
    "node", "kind", "slot", "t_ms", "blob", "column", "cells", "new_cells",
    "n_blobs", "role_group", "region", "bandwidth", "num_custody_columns",
]
_COLUMNS_COLS = ["node", "event", "slot", "t_ms", "column", "cells", "have", "required"]
_TRAFFIC_COLS = [
    "node", "event", "slot", "t_ms", "layer", "dir", "kind", "bytes",
    "recipients", "total_bytes", "new_cells", "estimated", "role_group",
]
_POOL_COLS = ["node", "slot", "t_ms", "pending", "included"]
_NODES_COLS = [
    "node", "roles", "region", "bandwidth", "is_cl_node", "is_builder",
    "is_zk_attester", "is_validator", "is_spammer", "role_group", "num_custody_columns",
]


# ---- primitive parsing ---------------------------------------------------------

def _coerce(value: str):
    """Turn a raw token into a python scalar/list. Numbers stay numeric; ``NA`` and
    ``true``/``false`` map to None/bool; bracketed lists become python lists."""
    if value == "NA":
        return None
    if value in ("true", "false"):
        return value == "true"
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        return [p for p in inner.split(",") if p] if inner else []
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _parse_event_line(line: str) -> dict | None:
    m = _EVENT_RE.search(line)
    if not m:
        return None
    fields = {k: _coerce(v) for k, v in _KV_RE.findall(m.group(1))}
    return fields if "kind" in fields else None


def _host_log(host_dir: Path) -> Path | None:
    """The blob-sim stdout log inside a host directory (name carries the round id)."""
    matches = sorted(host_dir.glob("blob-sim.*.stdout"))
    return matches[0] if matches else None


def _parse_all_events(hosts_dir: Path) -> pd.DataFrame:
    rows: list[dict] = []
    if hosts_dir.is_dir():
        for host_dir in sorted(hosts_dir.iterdir()):
            if not host_dir.is_dir():
                continue
            log = _host_log(host_dir)
            if not log:
                continue
            text = _ANSI.sub("", log.read_text(errors="replace"))
            for line in text.splitlines():
                if "EVENT " not in line:
                    continue
                ev = _parse_event_line(line)
                if ev is not None:
                    ev["node"] = host_dir.name
                    rows.append(ev)
    return pd.DataFrame(rows)


def _parse_metric_lines(hosts_dir: Path) -> pd.DataFrame:
    """Per-slot per-node bandwidth from `METRIC` lines — the §5 fallback when
    per-message `traffic` events are absent (e.g. a run made before that
    instrumentation). Each METRIC line yields four aggregate rows (el/cl x in/out)."""
    rows: list[dict] = []
    fanout = [
        ("el", "out", "el_bytes_sent"), ("el", "in", "el_bytes_received"),
        ("cl", "out", "cl_bytes_sent"), ("cl", "in", "cl_bytes_received"),
    ]
    if hosts_dir.is_dir():
        for host_dir in sorted(hosts_dir.iterdir()):
            if not host_dir.is_dir():
                continue
            log = _host_log(host_dir)
            if not log:
                continue
            text = _ANSI.sub("", log.read_text(errors="replace"))
            for line in text.splitlines():
                if "METRIC " not in line:
                    continue
                m = _METRIC_RE.search(line)
                if not m:
                    continue
                fields = {k: _coerce(v) for k, v in _KV_RE.findall(m.group(1))}
                for layer, direction, key in fanout:
                    rows.append({
                        "node": host_dir.name, "event": "slot_metric", "slot": fields.get("slot"),
                        "t_ms": pd.NA, "layer": layer, "dir": direction, "kind": "aggregate",
                        "bytes": fields.get(key),
                    })
    return pd.DataFrame(rows)


# ---- topology / config ---------------------------------------------------------

def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _parse_gml(path: Path) -> tuple[dict[int, dict], list[dict]]:
    """Return (node_id -> attrs, edges). Minimal GML reader for our generator's
    output: ``node [ id .. label .. region .. ]`` and ``edge [ source .. target ..
    latency "N ms" .. ]``."""
    nodes: dict[int, dict] = {}
    edges: list[dict] = []
    if not path.is_file():
        return nodes, edges
    text = path.read_text(errors="replace")
    # Split into `node [...]` / `edge [...]` blocks.
    for kind, body in re.findall(r"(node|edge)\s*\[(.*?)\]", text, re.DOTALL):
        attrs: dict = {}
        for key, val in re.findall(r'(\w+)\s+(".*?"|\S+)', body):
            attrs[key] = val.strip('"')
        if kind == "node" and "id" in attrs:
            nid = int(attrs["id"])
            nodes[nid] = {
                "label": attrs.get("label", str(nid)),
                "region": attrs.get("region", ""),
                "bandwidth": attrs.get("host_bandwidth_up", ""),
            }
        elif kind == "edge" and "source" in attrs and "target" in attrs:
            lat = attrs.get("latency", "0 ms").split()[0]
            try:
                lat_ms = float(lat)
            except ValueError:
                lat_ms = float("nan")
            edges.append(
                {"source": int(attrs["source"]), "target": int(attrs["target"]), "latency_ms": lat_ms}
            )
    return nodes, edges


def region_latency_matrix(run_dir: str | Path | None = None) -> pd.DataFrame:
    """Empirical inter-region latency matrix (mean over host-to-host GML edges),
    for §1. Rows/cols are regions; values are mean one-way edge latency in ms."""
    run_dir = _resolve_run_dir(run_dir)
    gml_nodes, edges = _parse_gml(run_dir / "topology.gml")
    id_region = {nid: a["region"] for nid, a in gml_nodes.items() if a["region"]}
    pairs: dict[tuple[str, str], list[float]] = {}
    for e in edges:
        rs, rt = id_region.get(e["source"]), id_region.get(e["target"])
        if not rs or not rt:
            continue  # switch node / missing region
        pairs.setdefault((rs, rt), []).append(e["latency_ms"])
        if rs != rt:
            pairs.setdefault((rt, rs), []).append(e["latency_ms"])
    regions = sorted({r for pair in pairs for r in pair})
    mat = pd.DataFrame(index=regions, columns=regions, dtype=float)
    for (rs, rt), vals in pairs.items():
        clean = [v for v in vals if v == v]
        if clean:
            mat.loc[rs, rt] = sum(clean) / len(clean)
    return mat


def _role_group(host: str, is_spammer: bool, is_builder: bool, is_zk: bool) -> str:
    if is_spammer:
        return "blob-spammer"
    if is_builder:
        return "proposer/builder"
    if is_zk:
        return "zk-attester"
    return "validator"


# ---- run-dir resolution --------------------------------------------------------

def _resolve_run_dir(run_dir: str | Path | None) -> Path:
    if run_dir:
        p = Path(run_dir)
    elif os.environ.get("BLOBSIM_RUN_DIR"):
        p = Path(os.environ["BLOBSIM_RUN_DIR"])
    else:
        # Default to the configured output dir, else repo-root/shadow-output.
        cfg = _read_config()
        p = Path(cfg.get("output", {}).get("dir", "shadow-output"))
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


def _read_config(config_path: str | Path | None = None) -> dict:
    path = Path(config_path) if config_path else REPO_ROOT / "blobsim.toml"
    if not path.is_absolute():
        path = REPO_ROOT / path
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return {}


# ---- frame builders ------------------------------------------------------------

def _build_nodes(run_dir: Path, events: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    regions = _read_json(run_dir / "regions.json")
    bandwidths = _read_json(run_dir / "bandwidths.json")
    gml_nodes, _ = _parse_gml(run_dir / "topology.gml")
    label_region = {a["label"]: a["region"] for a in gml_nodes.values()}
    label_bw = {a["label"]: a["bandwidth"] for a in gml_nodes.values()}

    zk_attesters = int(cfg.get("topology", {}).get("zk_attesters", 0))

    # Custody-column count observed per node (from readiness), if present.
    custody_by_node: dict[str, int] = {}
    if not events.empty and "num_custody_columns" in events:
        rd = events[events["kind"] == "readiness"]
        if not rd.empty:
            custody_by_node = (
                pd.to_numeric(rd["num_custody_columns"], errors="coerce")
                .groupby(rd["node"]).max().dropna().astype(int).to_dict()
            )

    # Real hosts only: regions.json/bandwidths.json list the blob-sim hosts; the
    # GML additionally carries a `switch` network device, which is not a host.
    event_hosts = set(events["node"].unique()) if not events.empty else set()
    hosts = sorted((set(regions) | set(bandwidths) | event_hosts) - {"switch"})
    rows = []
    zk_seen = 0
    for host in hosts:
        is_spammer = host.startswith("spammer")
        is_builder = host == "proposer"
        is_validator = host.startswith("validator")
        # zk-attesters are the first `zk_attesters` validators (validator000..).
        is_zk = False
        if is_validator and zk_seen < zk_attesters:
            is_zk = True
            zk_seen += 1
        roles = (
            ["blob-spammer"] if is_spammer
            else ["proposer", "builder"] if is_builder
            else (["validator", "zk-attester"] if is_zk else ["validator"])
        )
        default_custody = 128 if is_builder else 8
        rows.append({
            "node": host,
            "roles": roles,
            "region": regions.get(host) or label_region.get(host, ""),
            "bandwidth": bandwidths.get(host) or label_bw.get(host, ""),
            "is_cl_node": not is_spammer,
            "is_builder": is_builder,
            "is_zk_attester": is_zk,
            "is_validator": is_validator,
            "is_spammer": is_spammer,
            "role_group": _role_group(host, is_spammer, is_builder, is_zk),
            "num_custody_columns": custody_by_node.get(host, default_custody),
        })
    return pd.DataFrame(rows, columns=_NODES_COLS)


def _empty(cols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=cols)


def _select(events: pd.DataFrame, kinds: set[str], cols: list[str]) -> pd.DataFrame:
    """Rows of the given kinds, reindexed to the declared columns (missing -> NaN)."""
    if events.empty:
        return _empty(cols)
    sub = events[events["kind"].isin(kinds)].copy()
    if sub.empty:
        return _empty(cols)
    sub["event"] = sub["kind"]
    for c in cols:
        if c not in sub:
            sub[c] = pd.NA
    return sub[cols].reset_index(drop=True)


def _build_arrivals(events: pd.DataFrame, nodes: pd.DataFrame) -> pd.DataFrame:
    """Per-node blob/column receipts. The event line is `kind=arrival` with the
    receipt sub-type in `atype` (full_payload / custody_cells / partial); expose
    that sub-type as the frame's `kind` column."""
    if events.empty or "kind" not in events:
        return _empty(_ARRIVALS_COLS)
    sub = events[events["kind"] == "arrival"].copy()
    if sub.empty:
        return _empty(_ARRIVALS_COLS)
    sub["kind"] = sub["atype"] if "atype" in sub else pd.NA
    for c in _ARRIVALS_COLS:
        if c not in sub:
            sub[c] = pd.NA
    sub = sub[_ARRIVALS_COLS].reset_index(drop=True)
    return _attach_node_attrs(sub, nodes, _ARRIVALS_COLS)


def _attach_node_attrs(frame: pd.DataFrame, nodes: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Left-join role_group/region/bandwidth/num_custody_columns onto a frame."""
    if frame.empty:
        return frame
    attrs = nodes[["node", "role_group", "region", "bandwidth", "num_custody_columns"]]
    merged = frame.drop(columns=[c for c in ["role_group", "region", "bandwidth", "num_custody_columns"] if c in frame], errors="ignore")
    merged = merged.merge(attrs, on="node", how="left")
    for c in cols:
        if c not in merged:
            merged[c] = pd.NA
    return merged[cols]


def _normalise_epoch(events: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """Subtract the run epoch (median slot_start@slot0 t_ms) from every ms field so
    slot 0 begins at 0 and ``t_ms - slot*SLOT_MS`` is ms-into-slot."""
    if events.empty:
        return events, 0.0
    for col in ["t_ms", "slot", "block_t_ms", "envelope_t_ms", "custody_complete_t_ms"]:
        if col in events:
            events[col] = pd.to_numeric(events[col], errors="coerce")
    starts = events[(events["kind"] == "slot_start") & (events.get("slot") == 0)]
    epoch = float(starts["t_ms"].median()) if not starts.empty else (
        float(events["t_ms"].min()) if "t_ms" in events and events["t_ms"].notna().any() else 0.0)
    for col in ["t_ms", "block_t_ms", "envelope_t_ms", "custody_complete_t_ms"]:
        if col in events:
            events[col] = events[col] - epoch
    return events, epoch


def load_run(run_dir: str | Path | None = None) -> dict[str, pd.DataFrame]:
    run_dir = _resolve_run_dir(run_dir)
    cfg = _read_config()

    events = _parse_all_events(run_dir / "shadow.data" / "hosts")
    events, epoch_ms = _normalise_epoch(events)

    nodes = _build_nodes(run_dir, events, cfg)

    slots = _select(
        events,
        {"slot_start", "slot_end", "block_published", "envelope_published",
         "columns_seeded", "blob_offered", "readiness"},
        _SLOTS_COLS,
    )
    slots = _attach_node_attrs(slots, nodes, _SLOTS_COLS)

    arrivals = _build_arrivals(events, nodes)

    columns = _select(events, {"column_complete", "custody_complete", "column_status"}, _COLUMNS_COLS)

    # traffic events carry the message kind as `mkind` (the reserved `kind=` token
    # names the event type), so map it into the frame's `kind` column here.
    traffic_ev = events[events["kind"] == "traffic"].copy() if not events.empty else pd.DataFrame()
    if not traffic_ev.empty:
        traffic_ev["event"] = "traffic"
        traffic_ev["kind"] = traffic_ev["mkind"] if "mkind" in traffic_ev else "message"
        for c in _TRAFFIC_COLS:
            if c not in traffic_ev:
                traffic_ev[c] = pd.NA
        traffic = traffic_ev[_TRAFFIC_COLS].reset_index(drop=True)
    else:
        traffic = _empty(_TRAFFIC_COLS)
    traffic = _attach_node_attrs(traffic, nodes, _TRAFFIC_COLS)
    # Fallback: per-slot aggregate bandwidth from METRIC lines (tagged slot_metric),
    # so §5 has data even when per-message traffic events weren't emitted.
    metric_traffic = _parse_metric_lines(run_dir / "shadow.data" / "hosts")
    if not metric_traffic.empty:
        metric_traffic = _attach_node_attrs(metric_traffic, nodes, _TRAFFIC_COLS)
        traffic = pd.concat([traffic, metric_traffic], ignore_index=True)

    pool = _select(events, {"pool"}, _POOL_COLS)

    sim = cfg.get("sim", {})
    top = cfg.get("topology", {})
    # Prefer the per-run seed the launcher recorded (multi-run: run i uses seed+i),
    # so §0 and the seed-driven slot sampling reflect this specific run.
    run_meta = _read_json(run_dir / "run-meta.json")
    meta = pd.DataFrame([{
        "out_dir": str(run_dir),
        "config_path": str(REPO_ROOT / "blobsim.toml"),
        "seed": run_meta.get("seed", sim.get("seed")),
        "slots": sim.get("slots"),
        "blobs_per_slot": sim.get("blobs_per_slot"),
        "exec_payload_size_kib": sim.get("exec_payload_size_kib"),
        "enable_partial_columns": sim.get("enable_partial_columns"),
        "disable_get_blobs": sim.get("disable_get_blobs"),
        "blocks_in_blobs": sim.get("blocks_in_blobs"),
        "validators": top.get("validators"),
        "zk_attesters": top.get("zk_attesters"),
        "blob_spammers": top.get("blob_spammers"),
        "epoch_ms": epoch_ms,
    }])

    return {
        "traffic": traffic,
        "arrivals": arrivals,
        "columns": columns,
        "pool": pool,
        "slots": slots,
        "nodes": nodes,
        "meta": meta,
    }


def load_baseline(baseline_dir: str | Path | None = None) -> dict | None:
    """Optional paired-baseline run for reduction comparisons. Returns None unless
    ``BLOBSIM_BASELINE_DIR`` (or an explicit path) points at a run directory."""
    baseline_dir = baseline_dir or os.environ.get("BLOBSIM_BASELINE_DIR")
    if not baseline_dir:
        return None
    p = Path(baseline_dir)
    if not p.is_absolute():
        p = REPO_ROOT / p
    if not p.is_dir():
        return None
    return load_run(p)


def compare_run_meta(meta: pd.DataFrame, baseline_meta: pd.DataFrame) -> list[str]:
    """Human-readable warnings where run and baseline configs differ on keys that
    would make a paired comparison misleading. Empty list == cleanly paired."""
    if meta is None or baseline_meta is None or meta.empty or baseline_meta.empty:
        return ["missing meta for comparison"]
    a, b = meta.iloc[0], baseline_meta.iloc[0]
    warnings = []
    for key in ["seed", "slots", "blobs_per_slot", "exec_payload_size_kib", "validators"]:
        if a.get(key) != b.get(key):
            warnings.append(f"{key} differs: run={a.get(key)} baseline={b.get(key)}")
    return warnings
