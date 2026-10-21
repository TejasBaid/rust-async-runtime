#!/usr/bin/env bash
# Reproduces the HTTP numbers in the README.
#
# Starts the weft-http example server and the Tokio baseline (same parser,
# router and encoder, different runtime) on OS-assigned ports, then drives
# each with `oha` (if installed) and the bundled pipelining load generator.
#
#   scripts/bench_http.sh [WORKERS] [CONNECTIONS] [DURATION_SECS]
set -euo pipefail

WORKERS=${1:-4}
CONNS=${2:-64}
DURATION=${3:-10}
cd "$(dirname "$0")/.."

cargo build --release -q -p weft-http --example server
cargo build --release -q -p weft-bench --example tokio_http --bin loadgen

tmp=$(mktemp -d)
pids=()
cleanup() {
  if [ ${#pids[@]} -gt 0 ]; then kill "${pids[@]}" 2>/dev/null || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT

# start NAME BINARY: launches a server on an OS-assigned port (in this shell,
# so its pid is recorded for cleanup) and stores its address in $tmp/NAME.addr.
start() {
  "$2" 127.0.0.1:0 "$WORKERS" 2>"$tmp/$1.log" >/dev/null &
  pids+=($!)
  for _ in $(seq 50); do
    if grep -o '127\.0\.0\.1:[0-9]*' "$tmp/$1.log" >"$tmp/$1.addr" 2>/dev/null; then
      return
    fi
    sleep 0.1
  done
  echo "server $1 did not start" >&2
  exit 1
}

start weft target/release/examples/server
start tokio target/release/examples/tokio_http
WEFT=$(cat "$tmp/weft.addr")
TOKIO=$(cat "$tmp/tokio.addr")
echo "weft  @ $WEFT"
echo "tokio @ $TOKIO"
echo "workers=$WORKERS connections=$CONNS duration=${DURATION}s"

for path in /plaintext /json; do
  for target in "weft $WEFT" "tokio $TOKIO"; do
    set -- $target
    if command -v oha >/dev/null; then
      echo "== oha $1 $path"
      oha -z "${DURATION}s" -c "$CONNS" --no-tui "http://$2$path" 2>/dev/null \
        | grep -E "Requests/sec|Slowest|Fastest|Average|50.00%|90.00%|99.00%|99.90%|Success rate"
    fi
  done
done

for depth in 1 16; do
  for target in "weft $WEFT" "tokio $TOKIO"; do
    set -- $target
    echo "== loadgen $1 /plaintext pipeline=$depth"
    target/release/loadgen --addr "$2" --path /plaintext -c "$CONNS" -d "$DURATION" -p "$depth"
  done
done
