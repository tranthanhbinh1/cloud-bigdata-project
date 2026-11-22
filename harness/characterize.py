#!/usr/bin/env python3
"""Characterization gate: run each app once under C0 (no compression anywhere) and report CPU / disk /
network per machine (Prometheus) plus byte counts (Spark event log), to show that each app has the
profile it is supposed to have (I/O-bound, CPU-bound, balanced).

    python harness/characterize.py                      # all three apps at 5 GB
    python harness/characterize.py --apps IoBoundEtl --size 1gb
    python harness/characterize.py --conf spark.sql.shuffle.partitions=128   # extra --conf (repeatable)

Stdlib only. Needs the cluster up and `docker --context mac` working (see deploy/scripts/).
Results: results/characterization/<size>_<app>.json
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLIENT = "cb-client"
HDFS = "hdfs://cb-pc:8020"
PROM = "http://cb-pc:9090"
WORKER_CORES = 4
# Measured Wi-Fi capacity (deploy/scripts/netcheck.sh, 4 streams): Mac->PC = the PC's receive side, PC->Mac = its transmit side.
UPLINK_MBIT, DOWNLINK_MBIT = 280.0, 535.0
APPS = {"IoBoundEtl": "A1-io", "CpuBoundKMeans": "A2-cpu", "BalancedRouteStats": "A3-balanced"}

# C0: no storage compression, no communication compression of any kind.
C0_CONF = [
    "spark.sql.parquet.compression.codec=uncompressed",
    "spark.shuffle.compress=false",
    "spark.shuffle.spill.compress=false",
    "spark.broadcast.compress=false",
    "spark.rdd.compress=false",
]
MACHINES = ("pc", "mac")


def sh(cmd: list[str], check=True, capture=True) -> str:
    r = subprocess.run(cmd, capture_output=capture, text=True)
    if check and r.returncode:
        raise RuntimeError(f"{' '.join(cmd)}\n{r.stderr[-2000:]}")
    return r.stdout


def dx(*args: str, check=True) -> str:
    return sh(["docker", "exec", CLIENT, *args], check=check)


def drop_caches() -> None:
    for ctx in ([], ["mac"]):
        sh([str(ROOT / "deploy/scripts/drop-caches.sh"), *ctx])


def prom(query: str, t: float) -> list[dict]:
    out = dx("curl", "-sG", f"{PROM}/api/v1/query", "--data-urlencode", f"query={query}", "--data-urlencode", f"time={t}")
    return json.loads(out)["data"]["result"]


def prom_range(query: str, t0: float, t1: float, step: int = 2) -> list[dict]:
    out = dx("curl", "-sG", f"{PROM}/api/v1/query_range", "--data-urlencode", f"query={query}",
             "--data-urlencode", f"start={t0}", "--data-urlencode", f"end={t1}", "--data-urlencode", f"step={step}")
    return json.loads(out)["data"]["result"]


def by_machine(series: list[dict]) -> dict[str, float]:
    return {s["metric"].get("machine", "?"): float(s["value"][1]) for s in series}


def cpu_stats(t0: float, t1: float) -> dict:
    """Host busy cores (node-exporter; on the Mac this is the Docker Desktop VM) over the run: mean and peak."""
    res = {}
    for m in MACHINES:
        q = f'sum(rate(node_cpu_seconds_total{{machine="{m}",mode!="idle"}}[6s]))'
        vals = [float(v[1]) for s in prom_range(q, t0, t1) for v in s["values"]]
        if vals:
            res[m] = {"mean_busy_cores": round(sum(vals) / len(vals), 2), "peak_busy_cores": round(max(vals), 2)}
    return res


def counter_delta(metric_selector: str, t1: float, dur: float) -> dict[str, float]:
    return by_machine(prom(f"sum by (machine) (increase({metric_selector}[{int(dur) + 2}s]))", t1))


def eventlog_totals(app_id: str) -> dict:
    """Sum the task metrics from the driver's event log (written to /spark-events in the client container)."""
    # Spark 4 writes rolling event logs: a directory of events_<n>_<app-id>[.inprogress] files
    raw = dx("bash", "-c", f"cat /spark-events/eventlog_v2_{app_id}/events_* 2>/dev/null")
    tot = dict(tasks=0, run_ms=0, cpu_ns=0, gc_ms=0, input_b=0, output_b=0, shuf_w_b=0, shuf_w_ms=0,
               shuf_r_remote_b=0, shuf_r_local_b=0, fetch_wait_ms=0, spill_b=0, local=0, nonlocal_=0, by_host={})
    for line in raw.splitlines():
        if '"SparkListenerTaskEnd"' not in line:
            continue
        ev = json.loads(line)
        tm, ti = ev.get("Task Metrics") or {}, ev.get("Task Info", {})
        if not tm:
            continue
        tot["tasks"] += 1
        h = tot["by_host"].setdefault(ti.get("Host", "?"), {"cpu_s": 0.0, "tasks": 0})
        h["cpu_s"] += tm.get("Executor CPU Time", 0) / 1e9
        h["tasks"] += 1
        tot["run_ms"] += tm.get("Executor Run Time", 0)
        tot["cpu_ns"] += tm.get("Executor CPU Time", 0)
        tot["gc_ms"] += tm.get("JVM GC Time", 0)
        tot["input_b"] += tm.get("Input Metrics", {}).get("Bytes Read", 0)
        tot["output_b"] += tm.get("Output Metrics", {}).get("Bytes Written", 0)
        sw, sr = tm.get("Shuffle Write Metrics", {}), tm.get("Shuffle Read Metrics", {})
        tot["shuf_w_b"] += sw.get("Shuffle Bytes Written", 0)
        tot["shuf_w_ms"] += sw.get("Shuffle Write Time", 0) / 1e6
        tot["shuf_r_remote_b"] += sr.get("Remote Bytes Read", 0)
        tot["shuf_r_local_b"] += sr.get("Local Bytes Read", 0)
        tot["fetch_wait_ms"] += sr.get("Fetch Wait Time", 0)
        tot["spill_b"] += tm.get("Disk Bytes Spilled", 0)
        if ti.get("Locality") in ("PROCESS_LOCAL", "NODE_LOCAL"):
            tot["local"] += 1
        else:
            tot["nonlocal_"] += 1
    return tot


def run_app(app: str, size: str, extra_conf: list[str], tag: str, app_args: list[str] = ()) -> dict:
    src = f"{HDFS}/data/taxi/{size}/uncompressed"
    out = f"{HDFS}/tmp/characterize/{app}_{size}_{tag}"
    dx("hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", out, check=False)
    drop_caches()
    time.sleep(5)  # let the cluster go quiet so the window starts from idle
    conf = [x for kv in (C0_CONF + extra_conf) for x in ("--conf", kv)]
    cmd = ["docker", "exec", CLIENT, "spark-submit", "--class", f"compbench.apps.{app}", *conf,
           "/jars/scala-2.13/compbench.jar", "--input", src, "--output", out, "--run-id", f"char-{tag}",
           *[x for kv in app_args for x in (f"--{kv.split('=', 1)[0]}", kv.split("=", 1)[1])]]
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True)
    t1 = time.time()
    if r.returncode:
        raise RuntimeError(f"{app} failed:\n{(r.stdout + r.stderr)[-3000:]}")
    app_id = re.search(r"app-\d{14}-\d{4}", r.stdout + r.stderr).group(0)
    time.sleep(6)  # let the last 2 s scrapes land
    dur = t1 - t0
    sel = lambda m, dev="": f'{m}{{{dev}}}'
    res = {
        "app": app, "size": size, "app_id": app_id, "wall_s": round(dur, 1),
        "cpu": cpu_stats(t0, t1 + 4),
        # Only the PC's LAN interface is authoritative: on the Mac, Docker Desktop host networking hides flows that the PC
        # opens into the Mac from the VM's eth0 and shows Mac-local hairpin traffic instead, so Mac counters are unreliable.
        "net_pc_lan_tx_mb": round(counter_delta('node_network_transmit_bytes_total{device="enp7s0",machine="pc"}', t1 + 4, dur).get("pc", 0) / 1e6, 1),
        "net_pc_lan_rx_mb": round(counter_delta('node_network_receive_bytes_total{device="enp7s0",machine="pc"}', t1 + 4, dur).get("pc", 0) / 1e6, 1),
        "disk_read_mb": {m: round(v / 1e6, 1) for m, v in counter_delta('node_disk_read_bytes_total', t1 + 4, dur).items()},
        "disk_write_mb": {m: round(v / 1e6, 1) for m, v in counter_delta('node_disk_written_bytes_total', t1 + 4, dur).items()},
        "eventlog": eventlog_totals(app_id),
    }
    # half-duplex Wi-Fi: estimated share of airtime = uplink share + downlink share
    res["link_airtime_pct"] = round(100 * (res["net_pc_lan_rx_mb"] * 8 / dur / UPLINK_MBIT + res["net_pc_lan_tx_mb"] * 8 / dur / DOWNLINK_MBIT), 1)
    dx("hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", out, check=False)
    return res


def summarize(r: dict) -> str:
    e = r["eventlog"]
    gb = lambda b: f"{b / 1e9:6.2f}"
    cpu = "  ".join(f"{m}: {c['mean_busy_cores']:.1f} mean / {c['peak_busy_cores']:.1f} peak busy cores" for m, c in r["cpu"].items())
    ex = "  ".join(f"{h}: {v['cpu_s']:.0f}s CPU = {100 * v['cpu_s'] / (r['wall_s'] * WORKER_CORES):.0f}% of {WORKER_CORES} cores, {v['tasks']} tasks"
                   for h, v in sorted(e["by_host"].items()))
    return (f"{r['app']:<20} {r['size']:>5}  wall {r['wall_s']:6.1f}s\n"
            f"{'':<20} executor CPU (event log): {ex}\n"
            f"{'':<20} host CPU (node-exporter): {cpu}\n"
            f"{'':<20} input {gb(e['input_b'])} GB  shuffle-write {gb(e['shuf_w_b'])} GB  shuffle-read(remote) {gb(e['shuf_r_remote_b'])} GB  "
            f"output {gb(e['output_b'])} GB  spill {gb(e['spill_b'])} GB\n"
            f"{'':<20} PC LAN: tx {r['net_pc_lan_tx_mb']} MB, rx {r['net_pc_lan_rx_mb']} MB  -> est. link airtime {r['link_airtime_pct']} %\n"
            f"{'':<20} host disk read MB {r['disk_read_mb']}  write MB {r['disk_write_mb']}  | task locality: local {e['local']}, non-local {e['nonlocal_']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apps", nargs="+", default=list(APPS), choices=list(APPS))
    ap.add_argument("--size", default="5gb")
    ap.add_argument("--conf", action="append", default=[], help="extra spark conf key=value (repeatable)")
    ap.add_argument("--app-arg", action="append", default=[], help="app argument key=value, e.g. k=1024 (repeatable)")
    ap.add_argument("--tag", default=time.strftime("%Y%m%d-%H%M%S"))
    a = ap.parse_args()
    outdir = ROOT / "results" / "characterization"
    outdir.mkdir(parents=True, exist_ok=True)
    for app in a.apps:
        print(f"\n=== {app} @ {a.size} (C0) ===", flush=True)
        res = run_app(app, a.size, a.conf, a.tag, a.app_arg)
        res["conf"] = C0_CONF + a.conf
        res["app_args"] = a.app_arg
        (outdir / f"{a.size}_{app}_{a.tag}.json").write_text(json.dumps(res, indent=1))
        print(summarize(res), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
