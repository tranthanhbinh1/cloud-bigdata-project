#!/usr/bin/env bash
# Flush the Linux page cache on a Docker host's kernel (the PC itself, or the Docker Desktop VM on the Mac).
# Usage: drop-caches.sh [docker-context]   (default context = current)
# A privileged throwaway container writes to /proc/sys/vm/drop_caches of the host kernel.
set -euo pipefail
ctx=()
[ -n "${1:-}" ] && ctx=(--context "$1")
docker "${ctx[@]}" run --rm --privileged alpine:3.20 sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
