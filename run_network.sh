#!/usr/bin/env bash
# run_network.sh — Launch a local blob-sim network without Shadow.
#
# Starts one proposer+builder and a configurable number of validator nodes.
# Non-builder validators choose sampler/provider fetch behavior per blob.
#
# Each node connects to --peers-per-node randomly chosen other nodes
# (always including the proposer/builder).
#
# Usage:
#   ./run_network.sh [OPTIONS]
#
# Options:
#   --validators N      Number of validator nodes     (default: 100)
#   --peers-per-node N  Random peers each node dials   (default: 3)
#   --slots N           Slots per node                 (default: 10)
#   --base-port N       First port (builder)           (default: 9000)
#   --log-dir DIR       Per-node log directory         (default: logs)
#   --release           Use release build
#   -h, --help          Show this help

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────
VALIDATORS=100
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
        --validators)      VALIDATORS=$2;      shift 2;;
        --peers-per-node)  PEERS_PER_NODE=$2;  shift 2;;
        --slots)           SLOTS=$2;           shift 2;;
        --base-port)       BASE_PORT=$2;       shift 2;;
        --log-dir)         LOG_DIR=$2;         shift 2;;
        --release)         RELEASE="--release"; shift;;
        -h|--help)         usage; exit 0;;
        *) echo "error: unknown option: $1" >&2; exit 1;;
    esac
done

TOTAL=$VALIDATORS

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
# Order: proposer+builder, then validators
PROPOSER_PORT=$BASE_PORT
ALL_PORTS=("$PROPOSER_PORT")

PORT=$((BASE_PORT + 1))
for _ in $(seq 1 "$TOTAL"); do
    ALL_PORTS+=("$PORT")
    PORT=$((PORT + 1))
done

# Convert to comma-separated string for the Python helper
ALL_PORTS_CSV=$(IFS=,; echo "${ALL_PORTS[*]}")

# Select random peers for a node. Always includes the builder, then fills
# remaining slots from other nodes. Uses Python for portable deterministic
# random selection. Prints one token per line (`--peer` then the multiaddr) so
# callers can read it into an array without word-splitting.
#   pick_peers <my_port> <seed>  →  prints peer flag tokens, one per line
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
    print('--peer')
    print(f'/ip4/127.0.0.1/udp/{p}/quic-v1')
" "$ALL_PORTS_CSV" "$my_port" "$PEERS_PER_NODE" "$my_seed"
}

# Read pick_peers output (one token per line) into the global `_PEERS` array.
# A while-read loop (not `arr=( $(...) )`) avoids fragile word-splitting and is
# portable to bash 3.2 (no `mapfile`).
#   collect_peers <my_port> <seed>  →  populates _PEERS
collect_peers() {
    _PEERS=()
    local tok
    while IFS= read -r tok; do
        _PEERS+=("$tok")
    done < <(pick_peers "$1" "$2")
}

# ── Launch proposer+builder ──────────────────────────────────────────
collect_peers "$PROPOSER_PORT" 1
start_node "proposer-builder" --role proposer --role builder \
    --port "$PROPOSER_PORT" --seed 1 --slots "$SLOTS" \
    "${_PEERS[@]}"

PORT=$((BASE_PORT + 1))
SEED=100

# ── Launch validators ────────────────────────────────────────────────
for i in $(seq 1 "$VALIDATORS"); do
    name="validator$(printf '%03d' "$i")"
    roles=(--role validator)
    collect_peers "$PORT" "$SEED"
    start_node "$name" "${roles[@]}" --port "$PORT" --seed "$SEED" --slots "$SLOTS" \
        "${_PEERS[@]}"
    PORT=$((PORT + 1))
    SEED=$((SEED + 1))
done

# ── Summary ──────────────────────────────────────────────────────────
TOTAL_NODES=$((1 + VALIDATORS))
echo "Network launched: ${TOTAL_NODES} nodes, ${PEERS_PER_NODE} peers/node"
echo "  1 proposer+builder (port ${PROPOSER_PORT})"
echo "  ${VALIDATORS} validators"
echo "  ${SLOTS} slots, logs in ${LOG_DIR}/"
echo ""
echo "Waiting for ${SLOTS} slots to complete (Ctrl+C to stop early)..."
wait
