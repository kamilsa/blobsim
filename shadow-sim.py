#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Launch a blob-sim network simulation under Shadow, inside Docker.

Reads a TOML config (default `blobsim.toml`), generates a geo-realistic Shadow
topology (`topology.gml`) and a `shadow.yaml` wiring every node's CL (QUIC) and EL
(TCP) peers, then runs the simulation either:
  - in Docker  ([run].runner = "docker"): builds an image with blob-sim on top of a
    Shadow base image and runs Shadow in a container; or
  - natively   ([run].runner = "native"): builds blob-sim with `cargo build --release`
    and runs a `shadow` binary from PATH.

Usage:
  uv run shadow-sim.py                      # build (if needed) + run with blobsim.toml
  uv run shadow-sim.py my.toml              # use a different config file
  uv run shadow-sim.py --dry-run            # generate shadow.yaml/topology.gml only
  uv run shadow-sim.py --rebuild            # force `docker build` first
  uv run shadow-sim.py my.toml --clean      # remove previous shadow.data/ first

Results land in `<output.dir>/shadow.data/hosts/<host>/blob-sim.1000.stdout`.
Analyse them with `create_notebook.py` (see the README).
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import random
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent

# Fixed per-host ports. Each Shadow host has its own IP, so ports never collide
# (unlike run_network.sh, which shares 127.0.0.1 and must vary the port).
CL_PORT = 9000   # libp2p QUIC (UDP)
EL_PORT = 9100   # devp2p-style TCP

# Where the blob-sim binary lives, depending on the runner:
#   docker → baked into the image (see docker/Dockerfile)
#   native → the host's release build
BLOB_SIM_BIN_DOCKER = "/opt/blobsim/blob-sim"
BLOB_SIM_BIN_NATIVE = str(REPO_ROOT / "target" / "release" / "blob-sim")

# Inter-region round-trip latencies in milliseconds, ported verbatim from
# lean-shadow-fuzzer/shadow_fuzzer/generate_shadow_topology.py.
REGIONS_LATENCY_MS: dict[str, dict[str, int]] = {
    "us-east": {"us-east": 20, "us-west": 60, "europe": 80, "asia": 150, "sa": 120, "africa": 180},
    "us-west": {"us-east": 60, "us-west": 20, "europe": 130, "asia": 110, "sa": 160, "africa": 200},
    "europe": {"us-east": 80, "us-west": 130, "europe": 15, "asia": 100, "sa": 170, "africa": 80},
    "asia": {"us-east": 150, "us-west": 110, "europe": 100, "asia": 20, "sa": 250, "africa": 160},
    "sa": {"us-east": 120, "us-west": 160, "europe": 170, "asia": 250, "sa": 25, "africa": 220},
    "africa": {"us-east": 180, "us-west": 200, "europe": 80, "asia": 160, "sa": 220, "africa": 30},
}


def log(msg: str) -> None:
    print(f"[shadow-sim] {msg}", flush=True)


def die(msg: str) -> None:
    print(f"[shadow-sim] error: {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


# ── Node model ───────────────────────────────────────────────────────────────

class Node:
    """One simulated host: a Shadow host + a blob-sim process."""

    def __init__(self, index: int, name: str, roles: list[str], is_spammer: bool):
        self.index = index          # also its GML vertex id / network_node_id
        self.name = name            # Shadow hostname
        self.roles = roles          # blob-sim --role values
        self.is_spammer = is_spammer
        self.ip = ""                # assigned later
        self.region = ""            # assigned later
        self.bandwidth = ""         # assigned later
        self.spammer_ordinal = 0    # --node-id for blob-spammers


def build_nodes(cfg: dict) -> list[Node]:
    """Ordered node list: proposer (0), validators, then blob-spammers."""
    validators = int(cfg["topology"]["validators"])
    spammers = int(cfg["topology"]["blob_spammers"])
    zk_attesters = int(cfg["topology"].get("zk_attesters", 0))
    if spammers == 0:
        log("warning: blob_spammers = 0 — no blobs will be originated; propagation "
            "will be empty. Set [topology].blob_spammers >= 1 for a meaningful run.")
    if zk_attesters > validators:
        die(f"zk_attesters ({zk_attesters}) exceeds validators ({validators})")

    nodes: list[Node] = []
    nodes.append(Node(0, "proposer", ["proposer", "builder"], is_spammer=False))

    # The first `zk_attesters` validators are zk-attesters (EIP-8142): they skip
    # the payload-envelope topic and receive payload data over the column subnets.
    vwidth = max(3, len(str(validators)))
    for i in range(validators):
        idx = len(nodes)
        roles = ["validator", "zk-attester"] if i < zk_attesters else ["validator"]
        nodes.append(Node(idx, f"validator{i:0{vwidth}d}", roles, is_spammer=False))

    swidth = max(3, len(str(spammers)))
    for i in range(spammers):
        idx = len(nodes)
        n = Node(idx, f"spammer{i:0{swidth}d}", ["blob-spammer"], is_spammer=True)
        n.spammer_ordinal = i
        nodes.append(n)

    # Assign sequential IPs from 11.0.0.1 (11.0.0.0/8 → ~16M addresses).
    base = ipaddress.ip_address("11.0.0.1")
    for n in nodes:
        n.ip = str(base + n.index)
    return nodes


# ── Geo topology (regions, bandwidths, GML) ──────────────────────────────────
# Ported from lean-shadow-fuzzer/shadow_fuzzer/generate_shadow_topology.py so the
# latency model matches the fuzzer's: weighted region + bandwidth assignment and a
# full inter-host mesh whose edge latencies come from REGIONS_LATENCY_MS + jitter.

def weighted_assign(weights: dict[str, float], count: int, rng: random.Random) -> list[str]:
    keys = list(weights.keys())
    vals = list(weights.values())
    return [rng.choices(keys, weights=vals, k=1)[0] for _ in range(count)]


def generate_gml(nodes: list[Node], jitter_ratio: float, packet_loss: float,
                 rng: random.Random) -> str:
    n = len(nodes)
    lines: list[str] = ["graph [", "  directed 0"]

    for node in nodes:
        lines += [
            "  node [",
            f"    id {node.index}",
            f'    host_bandwidth_up "{node.bandwidth}"',
            f'    host_bandwidth_down "{node.bandwidth}"',
            f'    label "{node.name}"',
            f'    region "{node.region}"',
            "  ]",
        ]

    switch_id = n
    lines += [
        "  node [",
        f"    id {switch_id}",
        '    host_bandwidth_up "10 Gbit"',
        '    host_bandwidth_down "10 Gbit"',
        '    label "switch"',
        "  ]",
    ]

    # Self-loop edges (host↔itself and switch↔itself) at 1 ms.
    for node in nodes:
        lines += ["  edge [", f"    source {node.index}", f"    target {node.index}",
                  '    latency "1 ms"', f"    packet_loss {packet_loss}", "  ]"]
    lines += ["  edge [", f"    source {switch_id}", f"    target {switch_id}",
              '    latency "1 ms"', f"    packet_loss {packet_loss}", "  ]"]

    # Node→switch edges: half the intra-region latency + jitter.
    for node in nodes:
        base_ms = REGIONS_LATENCY_MS[node.region][node.region] * 0.5
        jitter = base_ms * jitter_ratio * (rng.random() * 2 - 1)
        latency = max(0.5, base_ms + jitter)
        lines += ["  edge [", f"    source {node.index}", f"    target {switch_id}",
                  f'    latency "{max(1, round(latency))} ms"',
                  f"    packet_loss {packet_loss}", "  ]"]

    # Full inter-host mesh: geo latency + per-edge jitter (direct edges win under
    # use_shortest_path, so these set the actual host-to-host latencies).
    for i in range(n):
        ri = nodes[i].region
        for j in range(i + 1, n):
            rj = nodes[j].region
            base_ms = REGIONS_LATENCY_MS[ri].get(rj, 200)
            jitter = base_ms * jitter_ratio * (rng.random() * 2 - 1)
            latency = max(0.5, base_ms + jitter)
            lines += ["  edge [", f"    source {i}", f"    target {j}",
                      f'    latency "{max(1, round(latency))} ms"',
                      f"    packet_loss {packet_loss}", "  ]"]

    lines.append("]")
    return "\n".join(lines) + "\n"


# ── Peer wiring (CL over QUIC, EL over TCP) ───────────────────────────────────

def wire_cl_peers(nodes: list[Node], peers_per_node: int,
                  rng: random.Random) -> dict[int, list[str]]:
    """Each CL node dials `peers_per_node` random other CL nodes (a random mesh).

    Unlike run_network.sh's pick_peers, this does NOT force every node to dial the
    proposer: at scale that turns the proposer into an N-way hub whose gossipsub
    bookkeeping starves the node's EL networking task (and it's unrealistic — real
    p2p nodes don't all peer through one host). Gossipsub forms its own mesh over
    whatever connected graph this produces, so blocks still reach everyone.
    Blob-spammers have no CL and are skipped.
    """
    cl = [n for n in nodes if not n.is_spammer]
    out: dict[int, list[str]] = {}
    for node in cl:
        candidates = [p for p in cl if p.index != node.index]
        chosen = rng.sample(candidates, min(peers_per_node, len(candidates)))
        out[node.index] = [f"/ip4/{p.ip}/udp/{CL_PORT}/quic-v1" for p in chosen]
    return out


def wire_el_peers(nodes: list[Node], el_peers_per_node: int,
                  rng: random.Random) -> dict[int, list[str]]:
    """Every node dials `el_peers_per_node` EL peers, always incl. >=1 blob-spammer.

    This is what makes blobs flow: spammers originate them and CL nodes must have EL
    links to receive and re-announce them through the sparse blobpool. (run_network.sh
    omits EL wiring entirely.)
    """
    out: dict[int, list[str]] = {}
    for node in nodes:
        spammers = [p for p in nodes if p.is_spammer and p.index != node.index]
        others = [p for p in nodes if p.index != node.index]
        chosen: list[Node] = []
        if spammers:
            chosen.append(rng.choice(spammers))
        pool = [p for p in others if p.index not in {c.index for c in chosen}]
        remaining = min(el_peers_per_node - len(chosen), len(pool))
        chosen += rng.sample(pool, remaining) if remaining > 0 else []
        out[node.index] = [f"{p.ip}:{EL_PORT}" for p in chosen]
    return out


# ── shadow.yaml ──────────────────────────────────────────────────────────────

def build_args(node: Node, cfg: dict, cl_peers: list[str], el_peers: list[str]) -> str:
    sim = cfg["sim"]
    slots = int(sim["slots"])
    seed = int(sim["seed"]) + node.index   # unique, deterministic per node
    parts: list[str] = []
    for r in node.roles:
        parts += ["--role", r]

    if node.is_spammer:
        parts += ["--el-port", str(EL_PORT),
                  "--node-id", str(node.spammer_ordinal),
                  "--blobs-per-slot", str(int(sim["blobs_per_slot"]))]
    else:
        parts += ["--port", str(CL_PORT), "--el-port", str(EL_PORT),
                  "--exec-payload-size",
                  str(int(sim.get("exec_payload_size_kib", 128)) * 1024)]
        if sim.get("enable_partial_columns"):
            parts.append("--enable-partial-columns")
        if sim.get("disable_get_blobs"):
            parts.append("--disable-get-blobs")
        if sim.get("blocks_in_blobs"):
            parts.append("--blocks-in-blobs")

    parts += ["--seed", str(seed), "--slots", str(slots)]
    for p in cl_peers:
        parts += ["--peer", p]
    for p in el_peers:
        parts += ["--el-peer", p]
    return " ".join(parts)


def generate_shadow_yaml(nodes: list[Node], cfg: dict, gml_path: Path,
                         cl_peers: dict[int, list[str]],
                         el_peers: dict[int, list[str]],
                         blob_sim_bin: str) -> str:
    sim = cfg["sim"]
    slots = int(sim["slots"])
    stop_time = sim.get("stop_time") or f"{slots * 12 + 10}s"
    rust_log = str(sim.get("rust_log", "info"))

    out: list[str] = [
        "# Auto-generated by shadow-sim.py — do not edit by hand.",
        "general:",
        f"  stop_time: {stop_time}",
        "  model_unblocked_syscall_latency: true",
        "",
        "network:",
        "  use_shortest_path: true",
        "  graph:",
        "    type: gml",
        "    file:",
        f"      path: {gml_path}",
        "",
        "hosts:",
    ]
    for node in nodes:
        args = build_args(node, cfg, cl_peers.get(node.index, []), el_peers.get(node.index, []))
        out += [
            f"  {node.name}:",
            f"    network_node_id: {node.index}",
            f"    ip_addr: {node.ip}",
            f'    bandwidth_up: "{node.bandwidth}"',
            f'    bandwidth_down: "{node.bandwidth}"',
            "    processes:",
            f"    - path: {blob_sim_bin}",
            f'      args: "{args}"',
            "      start_time: 1s",
            # blob-sim exits 0 once it finishes its --slots (well before stop_time,
            # which acts as a hang timeout). The reference's long-running clients use
            # `running`; ours must expect a clean exit instead.
            "      expected_final_state: {exited: 0}",
            "      environment:",
            f'        RUST_LOG: "{rust_log}"',
        ]
    return "\n".join(out) + "\n"


# ── Docker ───────────────────────────────────────────────────────────────────

def image_exists(image: str) -> bool:
    return subprocess.run(["docker", "image", "inspect", image],
                          capture_output=True).returncode == 0


def docker_build(image: str, shadow_image: str) -> None:
    log(f"building image {image} on base {shadow_image} (docker build, arm64)...")
    subprocess.run(
        ["docker", "build", "--platform", "linux/arm64",
         "-f", str(REPO_ROOT / "docker" / "Dockerfile"),
         "--build-arg", f"SHADOW_IMAGE={shadow_image}",
         "-t", image, str(REPO_ROOT)],
        check=True,
    )


def cargo_build_release() -> None:
    if shutil.which("cargo") is None:
        die("`cargo` not found in PATH — needed to build blob-sim for the native runner.")
    log("building blob-sim (cargo build --release)...")
    subprocess.run(["cargo", "build", "--release"], cwd=REPO_ROOT, check=True)


def native_run_shadow(out_dir: Path) -> None:
    if shutil.which("shadow") is None:
        die("`shadow` not found in PATH. Install Shadow (https://shadow.github.io/) "
            'or use the Docker runner ([run].runner = "docker").')
    shadow_data = out_dir / "shadow.data"
    shadow_yaml = out_dir / "shadow.yaml"
    log("running Shadow natively...")
    subprocess.run(
        ["shadow", "-d", str(shadow_data), "--progress", "true", str(shadow_yaml)],
        cwd=out_dir, check=True,
    )


def docker_run_shadow(image: str, out_dir: Path) -> None:
    # Clear any stale container from a previous interrupted run.
    subprocess.run(["docker", "rm", "-f", "blobsim-shadow-run"],
                   check=False, capture_output=True)
    shadow_data = out_dir / "shadow.data"
    shadow_yaml = out_dir / "shadow.yaml"
    log("running Shadow in container...")
    subprocess.run(
        ["docker", "run", "--rm", "--name", "blobsim-shadow-run",
         "--platform", "linux/arm64",
         "--security-opt", "seccomp=unconfined",
         "--shm-size", "4g",
         "-v", f"{out_dir}:{out_dir}",
         "-w", str(out_dir),
         "--entrypoint", "/bin/bash", image,
         "-c", f"shadow -d {shadow_data} --progress true {shadow_yaml}"],
        check=True,
    )


# ── Post-run summary ─────────────────────────────────────────────────────────
# Parses the per-host logs Shadow leaves under shadow.data/ and reports run health
# at a glance: blocks produced, how widely they propagated, blob commitments, and
# EL/CL traffic — so you don't have to grep 100+ log files to see if a run did
# anything interesting.

_ANSI = re.compile(r"\x1b\[[0-9;]*[mK]")
_METRIC_FIELDS = (
    "gossip_sent", "gossip_received", "gossip_forwarded",
    "el_announces_sent", "el_announces_received",
    "el_requests_sent", "el_responses_sent", "el_responses_received",
)


def summarize(out_dir: Path) -> None:
    hosts_dir = out_dir / "shadow.data" / "hosts"
    if not hosts_dir.is_dir():
        log(f"no shadow.data/hosts under {out_dir} — nothing to summarize.")
        return

    blocks_produced: set[int] = set()          # slots the proposer built a block for
    commitments: dict[int, int] = {}           # slot -> blob commitments in that block
    block_recv: dict[int, int] = {}            # slot -> #hosts that received the block
    envelope_recv: dict[int, int] = {}         # slot -> #hosts that received the envelope
    validators = 0
    totals: dict[str, int] = {k: 0 for k in _METRIC_FIELDS}

    re_prop = re.compile(r"published beacon block proposal.*slot=(\d+) blobs=(\d+)")
    re_brecv = re.compile(r"received beacon block proposal slot=(\d+)")
    re_erecv = re.compile(r"received payload envelope slot=(\d+)")
    re_metric = {k: re.compile(rf"{k}=(\d+)") for k in _METRIC_FIELDS}

    for host in sorted(hosts_dir.iterdir()):
        stdout = host / "blob-sim.1000.stdout"
        if not stdout.is_file():
            continue
        text = _ANSI.sub("", stdout.read_text(errors="replace"))
        if host.name.startswith("validator"):
            validators += 1
        saw_block: set[int] = set()
        saw_env: set[int] = set()
        for line in text.splitlines():
            if (m := re_prop.search(line)):
                blocks_produced.add(int(m.group(1)))
                commitments[int(m.group(1))] = int(m.group(2))
            elif (m := re_brecv.search(line)):
                saw_block.add(int(m.group(1)))
            elif (m := re_erecv.search(line)):
                saw_env.add(int(m.group(1)))
            if "METRIC" in line:
                for k, rx in re_metric.items():
                    if (mm := rx.search(line)):
                        totals[k] += int(mm.group(1))
        for s in saw_block:
            block_recv[s] = block_recv.get(s, 0) + 1
        for s in saw_env:
            envelope_recv[s] = envelope_recv.get(s, 0) + 1

    log("── run summary ─────────────────────────────────────────────")
    log(f"blocks produced: {len(blocks_produced)}  |  validators: {validators}")
    if blocks_produced:
        log("per-slot propagation (block / envelope reach, blob commitments):")
        for s in sorted(blocks_produced):
            log(f"  slot {s:>2}: block {block_recv.get(s, 0):>3}/{validators}  "
                f"envelope {envelope_recv.get(s, 0):>3}/{validators}  "
                f"commitments={commitments.get(s, 0)}")
        total_commit = sum(commitments.values())
        if total_commit == 0:
            log("⚠  every block committed 0 blobs — the EL blob pipeline delivered "
                "nothing to the builder (see el_announces below).")
    log("EL blob announces:  sent={el_announces_sent}  received={el_announces_received}"
        .format(**totals))
    if totals["el_announces_sent"] and totals["el_announces_received"] * 5 < totals["el_announces_sent"]:
        log("⚠  received << sent — most blob-hash announces are not being delivered/"
            "re-propagated over EL.")
    log("EL requests: sent={el_requests_sent}  responses_sent={el_responses_sent}  "
        "responses_received={el_responses_received}".format(**totals))
    log("CL gossip:   sent={gossip_sent}  received={gossip_received}  "
        "forwarded={gossip_forwarded}".format(**totals))
    log("────────────────────────────────────────────────────────────")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Run blob-sim under Shadow in Docker.")
    ap.add_argument("config", nargs="?", default="blobsim.toml",
                    help="path to the TOML config (default: blobsim.toml)")
    ap.add_argument("--dry-run", action="store_true",
                    help="generate shadow.yaml + topology.gml only; no build or run")
    ap.add_argument("--rebuild", action="store_true", help="force `docker build` first")
    ap.add_argument("--clean", action="store_true",
                    help="remove a previous shadow.data/ before running")
    ap.add_argument("--summary-only", action="store_true",
                    help="skip generation/build/run; just summarize an existing run")
    args = ap.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = REPO_ROOT / cfg_path
    if not cfg_path.is_file():
        die(f"config not found: {cfg_path}")
    with cfg_path.open("rb") as f:
        cfg = tomllib.load(f)

    if args.summary_only:
        out_dir = Path(cfg["output"]["dir"])
        if not out_dir.is_absolute():
            out_dir = REPO_ROOT / out_dir
        summarize(out_dir)
        return

    # Runner: "docker" (build+run in a container) or "native" (build with cargo and
    # run a shadow binary from PATH). "local" is accepted as an alias for "native".
    runner = str(cfg.get("run", {}).get("runner", "docker")).lower()
    if runner == "local":
        runner = "native"
    if runner not in ("docker", "native"):
        die(f'[run].runner must be "docker" or "native", got "{runner}"')
    blob_sim_bin = BLOB_SIM_BIN_DOCKER if runner == "docker" else BLOB_SIM_BIN_NATIVE

    out_dir = Path(cfg["output"]["dir"])
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.clean:
        shutil.rmtree(out_dir / "shadow.data", ignore_errors=True)

    seed = int(cfg["sim"]["seed"])
    net = cfg["network"]
    jitter_ratio = float(net.get("jitter_ratio", 0.3))
    packet_loss = float(net.get("packet_loss", 0.0))
    region_weights = {k: float(v) for k, v in cfg["network"]["regions"].items()}
    bw_weights = {k: float(v) for k, v in cfg["network"]["bandwidths"].items()}

    nodes = build_nodes(cfg)
    n = len(nodes)
    if n > 250:
        log(f"warning: {n} nodes → a full inter-host mesh of {n * (n - 1) // 2} edges; "
            "topology generation and Shadow start-up may be slow.")

    # One seeded RNG for the geo topology (mirrors the reference ordering: regions,
    # bandwidths, then GML jitter); a separate stream for peer selection.
    geo_rng = random.Random(seed)
    regions = weighted_assign(region_weights, n, geo_rng)
    bandwidths = weighted_assign(bw_weights, n, geo_rng)
    for node, r, bw in zip(nodes, regions, bandwidths):
        node.region, node.bandwidth = r, bw

    gml = generate_gml(nodes, jitter_ratio, packet_loss, geo_rng)
    gml_path = out_dir / "topology.gml"
    gml_path.write_text(gml)

    # Debug artifacts, matching the fuzzer's outputs.
    (out_dir / "regions.json").write_text(
        json.dumps({node.name: node.region for node in nodes}, indent=2))
    (out_dir / "bandwidths.json").write_text(
        json.dumps({node.name: node.bandwidth for node in nodes}, indent=2))

    peer_rng = random.Random(seed + 1)
    cl_peers = wire_cl_peers(nodes, int(cfg["topology"]["peers_per_node"]), peer_rng)
    el_peers = wire_el_peers(nodes, int(cfg["topology"]["el_peers_per_node"]), peer_rng)

    shadow_yaml = generate_shadow_yaml(nodes, cfg, gml_path, cl_peers, el_peers, blob_sim_bin)
    yaml_path = out_dir / "shadow.yaml"
    yaml_path.write_text(shadow_yaml)

    validators = sum(1 for x in nodes if "validator" in x.roles)
    zk_attesters = sum(1 for x in nodes if "zk-attester" in x.roles)
    spammers = sum(1 for x in nodes if x.is_spammer)
    region_counts: dict[str, int] = {}
    for x in nodes:
        region_counts[x.region] = region_counts.get(x.region, 0) + 1
    log(f"generated {yaml_path}")
    log(f"  runner: {runner}")
    log(f"  {n} hosts: 1 proposer+builder, {validators} validators "
        f"(of which {zk_attesters} zk-attesters), {spammers} spammers")
    log(f"  regions: {region_counts}")

    if args.dry_run:
        log("dry run — skipping build and Shadow.")
        return

    if runner == "docker":
        image = cfg["docker"]["image_name"]
        shadow_image = cfg["docker"].get("shadow_image", "kamilsa/shadow-arm:tcpfix")
        if args.rebuild or cfg["docker"].get("rebuild") or not image_exists(image):
            docker_build(image, shadow_image)
        else:
            log(f"reusing existing image {image} (pass --rebuild to force a rebuild)")
        docker_run_shadow(image, out_dir)
    else:  # native
        cargo_build_release()
        native_run_shadow(out_dir)

    summarize(out_dir)

    log("done. Results under:")
    log(f"  {out_dir}/shadow.data/hosts/<host>/blob-sim.1000.stdout")
    log(f"Analyse with:  (cd {out_dir} && python {REPO_ROOT}/create_notebook.py)")


if __name__ == "__main__":
    main()
