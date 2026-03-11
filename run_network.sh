#!/usr/bin/env bash
# run_network.sh — Launch a local blob-sim network without Shadow.
#
# Starts one builder and a configurable number of samplers, providers,
# and PTC members. PTC members are distributed proportionally among
# samplers and providers (PTC is always combined with another role).
#
# Each node connects to --peers-per-node randomly chosen other nodes
# (always including the builder).
#
# Usage:
#   ./run_network.sh [OPTIONS]
#
# Options:
#   --samplers N        Number of sampler nodes       (default: 85)
#   --providers N       Number of provider nodes       (default: 15)
#   --ptc N             Number of PTC members          (default: 25)
#   --peers-per-node N  Random peers each node dials   (default: 3)
#   --slots N           Slots per node                 (default: 10)
#   --base-port N       First port (builder)           (default: 9000)
#   --log-dir DIR       Per-node log directory         (default: logs)
#   --release           Use release build
#   -h, --help          Show this help

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────
SAMPLERS=85
PROVIDERS=15
PTC=25
PEERS_PER_NODE=3
SLOTS=10
BASE_PORT=9000
LOG_DIR="logs"
RELEASE=""

# ── Argument parsing ─────────────────────────────────────────────────
usage() {
    sed -n '2,/^$/{ s/^# \?//; p }' "$0"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --samplers)        SAMPLERS=$2;        shift 2;;
        --providers)       PROVIDERS=$2;       shift 2;;
        --ptc)             PTC=$2;             shift 2;;
        --peers-per-node)  PEERS_PER_NODE=$2;  shift 2;;
        --slots)           SLOTS=$2;           shift 2;;
        --base-port)       BASE_PORT=$2;       shift 2;;
        --log-dir)         LOG_DIR=$2;         shift 2;;
        --release)         RELEASE="--release"; shift;;
        -h|--help)         usage; exit 0;;
        *) echo "error: unknown option: $1" >&2; exit 1;;
    esac
done

TOTAL=$((SAMPLERS + PROVIDERS))
if (( PTC > TOTAL )); then
    echo "error: --ptc ($PTC) cannot exceed samplers + providers ($TOTAL)" >&2
    exit 1
fi

# ── Build ────────────────────────────────────────────────────────────
echo "Building blob-sim${RELEASE:+ (release)}..."
cargo build $RELEASE --quiet

if [[ -n "$RELEASE" ]]; then
    BIN="./target/release/blob-sim"
else
    BIN="./target/debug/blob-sim"
fi

# ── Prepare logs ─────────────────────────────────────────────────────
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

# ── Process management ───────────────────────────────────────────────
PIDS=()

cleanup() {
    echo ""
    echo "Shutting down ${#PIDS[@]} processes..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT INT TERM

start_node() {
    local name=$1; shift
    local log="$LOG_DIR/${name}.log"
    RUST_LOG=info "$BIN" "$@" > "$log" 2>&1 &
    PIDS+=($!)
}

# ── Collect all node ports ───────────────────────────────────────────
# Order: proposer, builder, then samplers + providers
PROPOSER_PORT=$BASE_PORT
BUILDER_PORT=$((BASE_PORT + 1))
ALL_PORTS=("$PROPOSER_PORT" "$BUILDER_PORT")

PORT=$((BASE_PORT + 2))
for _ in $(seq 1 "$TOTAL"); do
    ALL_PORTS+=("$PORT")
    PORT=$((PORT + 1))
done

# Convert to comma-separated string for the Python helper
ALL_PORTS_CSV=$(IFS=,; echo "${ALL_PORTS[*]}")

# Select random peers for a node. Always includes the builder, then fills
# remaining slots from other nodes. Uses Python for portable deterministic
# random selection.
#   pick_peers <my_port> <seed>  →  prints --peer flags
pick_peers() {
    local my_port=$1 my_seed=$2
    python3 -c "
import random, sys
all_ports = [int(p) for p in sys.argv[1].split(',')]
my_port   = int(sys.argv[2])
builder   = all_ports[0]
n         = int(sys.argv[3])
seed      = int(sys.argv[4])

random.seed(seed)
candidates = [p for p in all_ports if p != my_port and p != builder]
extra = min(n - 1, len(candidates))
chosen = [builder] + random.sample(candidates, extra) if my_port != builder else random.sample(candidates, min(n, len(candidates)))
for p in chosen:
    print(f'--peer /ip4/127.0.0.1/udp/{p}/quic-v1')
" "$ALL_PORTS_CSV" "$my_port" "$PEERS_PER_NODE" "$my_seed"
}

# ── Distribute PTC proportionally ────────────────────────────────────
PTC_SAMPLERS=$(( (PTC * SAMPLERS + TOTAL / 2) / TOTAL ))
PTC_PROVIDERS=$(( PTC - PTC_SAMPLERS ))

# ── Launch proposer ──────────────────────────────────────────────────
PROPOSER_PEERS=( $(pick_peers "$PROPOSER_PORT" 1) )
start_node "proposer" --role proposer --port "$PROPOSER_PORT" --seed 1 --slots "$SLOTS" \
    "${PROPOSER_PEERS[@]}"

# ── Launch builder ───────────────────────────────────────────────────
BUILDER_PEERS=( $(pick_peers "$BUILDER_PORT" 2) )
start_node "builder" --role builder --port "$BUILDER_PORT" --seed 2 --slots "$SLOTS" \
    "${BUILDER_PEERS[@]}"

PORT=$((BASE_PORT + 2))
SEED=100

# ── Launch samplers ──────────────────────────────────────────────────
for i in $(seq 1 "$SAMPLERS"); do
    name="sampler$(printf '%03d' "$i")"
    roles=(--role sampler)
    if (( i <= PTC_SAMPLERS )); then
        roles+=(--role ptc)
        name="sampler-ptc$(printf '%03d' "$i")"
    fi
    PEER_FLAGS=( $(pick_peers "$PORT" "$SEED") )
    start_node "$name" "${roles[@]}" --port "$PORT" --seed "$SEED" --slots "$SLOTS" \
        "${PEER_FLAGS[@]}"
    PORT=$((PORT + 1))
    SEED=$((SEED + 1))
done

# ── Launch providers ─────────────────────────────────────────────────
for i in $(seq 1 "$PROVIDERS"); do
    name="provider$(printf '%03d' "$i")"
    roles=(--role provider)
    if (( i <= PTC_PROVIDERS )); then
        roles+=(--role ptc)
        name="provider-ptc$(printf '%03d' "$i")"
    fi
    PEER_FLAGS=( $(pick_peers "$PORT" "$SEED") )
    start_node "$name" "${roles[@]}" --port "$PORT" --seed "$SEED" --slots "$SLOTS" \
        "${PEER_FLAGS[@]}"
    PORT=$((PORT + 1))
    SEED=$((SEED + 1))
done

# ── Summary ──────────────────────────────────────────────────────────
TOTAL_NODES=$((2 + SAMPLERS + PROVIDERS))
echo "Network launched: ${TOTAL_NODES} nodes, ${PEERS_PER_NODE} peers/node"
echo "  1 proposer (port ${PROPOSER_PORT})"
echo "  1 builder (port ${BUILDER_PORT})"
echo "  ${SAMPLERS} samplers (${PTC_SAMPLERS} also PTC)"
echo "  ${PROVIDERS} providers (${PTC_PROVIDERS} also PTC)"
echo "  ${SLOTS} slots, logs in ${LOG_DIR}/"
echo ""
echo "Waiting for ${SLOTS} slots to complete (Ctrl+C to stop early)..."
wait
