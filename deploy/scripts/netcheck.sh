#!/usr/bin/env bash
# Measures the PC<->Mac LAN throughput with iperf3 (4 parallel streams, both directions) and prints ONE JSON line.
# Run on the PC. The iperf3 server runs in the cb-client container (PC network); the client runs in the Mac's
# DataNode container via the Mac Docker context ($MAC_CONTEXT, default "mac").
#   mac_to_pc = Mac uplink (the slower direction on Wi-Fi), pc_to_mac = downlink.
set -euo pipefail
export LC_ALL=C   # decimal point, not the locale's comma (output is JSON)
MAC_CONTEXT="${MAC_CONTEXT:-mac}"
SECS="${SECS:-8}"
STREAMS="${STREAMS:-4}"

run() {  # run <extra iperf3 flags> -> Mbit/s (receiver side, summed over streams)
  docker exec -d cb-client iperf3 -s -1 -p 5201 >/dev/null 2>&1
  sleep 1
  docker --context "$MAC_CONTEXT" exec cb-datanode-mac iperf3 -c cb-pc -p 5201 -P "$STREAMS" -t "$SECS" -J "$@" \
    | python3 -c 'import sys,json; print(round(json.load(sys.stdin)["end"]["sum_received"]["bits_per_second"]/1e6,1))'
}
up=$(run)
sleep 1
down=$(run -R)
printf '{"mac_to_pc_mbit_s":%s,"pc_to_mac_mbit_s":%s,"streams":%s}\n' "$up" "$down" "$STREAMS"
