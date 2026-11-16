#!/usr/bin/env bash
# Verifies the PC<->Mac Tailscale path is DIRECT (not DERP-relayed) and measures throughput.
# Run on the PC. Prints one JSON line; `--quiet` suppresses the human summary (used by the harness).
#   iperf3 server runs in the cb-client container on the PC; the client runs in the Mac's DataNode
#   container via the Mac Docker context ($MAC_CONTEXT, default "mac").
set -euo pipefail
MAC_CONTEXT="${MAC_CONTEXT:-mac}"
SECS="${SECS:-10}"

ping_out=$(docker exec cb-ts-pc tailscale ping --c 5 --until-direct=false cb-mac 2>&1 || true)
if echo "$ping_out" | grep -q "DERP"; then path="relayed"; else path="direct"; fi
latency=$(echo "$ping_out" | grep -oE 'in [0-9.]+ms' | tail -1 | grep -oE '[0-9.]+' || echo "null")

docker exec -d cb-client iperf3 -s -1 -p 5201 >/dev/null 2>&1
sleep 1
bps=$(docker --context "$MAC_CONTEXT" exec cb-datanode-mac iperf3 -c cb-pc -p 5201 -t "$SECS" -J \
        | python3 -c 'import sys,json; print(int(json.load(sys.stdin)["end"]["sum_received"]["bits_per_second"]))')

printf '{"path":"%s","latency_ms":%s,"iperf3_mbit_s":%.1f}\n' "$path" "${latency:-null}" "$(echo "$bps/1000000" | bc -l)"
[ "$path" = "direct" ] || { echo "WARNING: Tailscale path is relayed; throughput will be unrepresentative" >&2; exit 1; }
