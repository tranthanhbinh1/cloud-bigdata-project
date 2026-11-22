#!/usr/bin/env python3
"""Experiment runner: the full compression study, resumable and unattended.

Matrix (per app and dataset size):
  C0  no compression at either level
  C1  storage compression only:        snappy | lz4 | gzip | zstd   (Parquet on HDFS: read AND write)
  C2  communication compression only:  lz4 | snappy | zstd              (shuffle / spill / broadcast)
  C3  both:                            snappy+lz4 (fast pair), gzip+zstd (heavy pair)
A2 (CpuBoundKMeans) has no meaningful shuffle, so it only runs C0 and C1.

Each run: flush page caches on both machines -> spark-submit -> collect Spark event-log totals + Prometheus
CPU / disk / network -> results/raw/<run_id>/result.json. A run whose result.json exists is skipped, so the
campaign can be stopped and restarted. Order is randomized (seeded) within each dataset size.

    python harness/runner.py --dry-run                         # print the plan and a time estimate
    python harness/runner.py --sizes 0.5 --reps 1              # smoke test
    python harness/runner.py --sizes 0.5 2.5 5 10 --reps 3     # the real campaign
    python harness/runner.py --apps IoBoundEtl --configs 'C1|C0'   # regex filter on config names

Run it under `systemd-inhibit --what=idle:sleep` so the PC does not suspend (the Mac is kept awake over SSH).
"""
from __future__ import annotations

import argparse
import json
import random
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import characterize as ch  # noqa: E402  (shared helpers: Prometheus queries, event log, cache flush, ...)

ROOT = ch.ROOT
RAW = ROOT / "results" / "raw"
SIZES_DEFAULT = ["0.5", "2.5", "5", "10"]
STORAGE = ["snappy", "lz4", "gzip", "zstd"]   # Parquet "lz4" (Hadoop LZ4 via lz4-java); "lz4_raw" fails on the ARM64 Mac, see README
SHUFFLE = ["lz4", "snappy", "zstd"]
C3_PAIRS = [("snappy", "lz4"), ("gzip", "zstd")]
# rough seconds per nominal GB of input (measured at 5 GB under C0, 4 PC workers + 1 Mac worker) + fixed overhead per run,
# only for the --dry-run estimate
SEC_PER_GB = {"IoBoundEtl": 34, "CpuBoundKMeans": 15.5, "BalancedRouteStats": 19}
OVERHEAD_S = 12   # measured: ~9 s between runs (cache flush, waits, bookkeeping) + startup not covered by SEC_PER_GB


@dataclass(frozen=True)
class Cfg:
    name: str
    group: str
    storage: str   # "none" | codec
    shuffle: str   # "off"  | codec


def configs_for(app: str) -> list[Cfg]:
    cfgs = [Cfg("C0", "C0", "none", "off")]
    cfgs += [Cfg(f"C1-{s}", "C1", s, "off") for s in STORAGE]
    if app != "CpuBoundKMeans":
        cfgs += [Cfg(f"C2-{k}", "C2", "none", k) for k in SHUFFLE]
        cfgs += [Cfg(f"C3-{s}+{k}", "C3", s, k) for s, k in C3_PAIRS]
    return cfgs


def spark_confs(c: Cfg) -> list[str]:
    on = str(c.shuffle != "off").lower()
    confs = [
        f"spark.sql.parquet.compression.codec={'uncompressed' if c.storage == 'none' else c.storage}",
        f"spark.shuffle.compress={on}",
        f"spark.shuffle.spill.compress={on}",
        f"spark.broadcast.compress={on}",
        "spark.rdd.compress=false",
    ]
    if c.shuffle != "off":
        confs.append(f"spark.io.compression.codec={c.shuffle}")
    return confs


def dataset_path(size: str, c: Cfg) -> str:
    return f"{ch.HDFS}/data/taxi/{size}gb/{'uncompressed' if c.storage == 'none' else c.storage}"


def plan(sizes, apps, reps, cfg_re, seed) -> list[tuple]:
    out = []
    for size in sizes:
        runs = [(size, app, cfg, rep) for app in apps for cfg in configs_for(app)
                if (not cfg_re or re.search(cfg_re, cfg.name)) for rep in range(1, reps + 1)]
        random.Random(f"{seed}-{size}").shuffle(runs)   # randomized within a size, reproducible
        out += runs
    return out


def run_id(size, app, cfg, rep) -> str:
    return f"{size}gb_{app}_{cfg.name}_r{rep}"


# ---------------------------------------------------------------------------------------------- measurement

def eventlog_stages(app_id: str) -> dict:
    """Stage wall times from the event log: total, and the wall time of stages that wrote shuffle data."""
    raw = ch.dx("bash", "-c", f"cat /spark-events/eventlog_v2_{app_id}/events_* 2>/dev/null")
    shuffle_w: dict[int, int] = {}
    stages: dict[int, float] = {}
    for line in raw.splitlines():
        if '"SparkListenerTaskEnd"' in line:
            ev = json.loads(line)
            tm = ev.get("Task Metrics") or {}
            sid = ev.get("Stage ID")
            shuffle_w[sid] = shuffle_w.get(sid, 0) + tm.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
        elif '"SparkListenerStageCompleted"' in line:
            si = json.loads(line)["Stage Info"]
            if "Submission Time" in si and "Completion Time" in si:
                stages[si["Stage ID"]] = (si["Completion Time"] - si["Submission Time"]) / 1000.0
    return {"stage_wall_total_s": round(sum(stages.values()), 2),
            "shuffle_map_stage_wall_s": round(sum(d for s, d in stages.items() if shuffle_w.get(s, 0) > 0), 2),
            "n_stages": len(stages)}


# HDFS bookkeeping goes through WebHDFS on the NameNode (host network on this PC): ~50 ms per call instead of
# ~1.2 s for an `hdfs dfs` JVM start inside the client container.
WEBHDFS = "http://127.0.0.1:9870/webhdfs/v1"


def _webhdfs(path: str, op: str, method: str = "GET", **params) -> dict:
    p = urllib.parse.urlparse(path).path or path          # accept hdfs://host:port/x or /x
    q = urllib.parse.urlencode({"op": op, "user.name": "root", **params})
    req = urllib.request.Request(f"{WEBHDFS}{urllib.parse.quote(p)}?{q}", method=method)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def hdfs_bytes(path: str) -> int:
    try:
        return int(_webhdfs(path, "GETCONTENTSUMMARY")["ContentSummary"]["length"])
    except (urllib.error.URLError, KeyError, ValueError):
        return -1


def hdfs_rm(path: str) -> None:
    try:
        _webhdfs(path, "DELETE", method="DELETE", recursive="true")   # {"boolean": false} if absent: fine
    except urllib.error.URLError:
        pass


def assert_cluster() -> None:
    d = json.loads(ch.dx("curl", "-sf", "http://cb-pc:8080/json/"))
    alive = sorted(w["host"] for w in d["workers"] if w["state"] == "ALIVE")
    expected = sorted(["cb-mac"] + ["cb-pc"] * ch.PC_WORKERS)
    if alive != expected:
        raise RuntimeError(f"cluster not healthy: alive workers = {alive}, expected {expected}")
    # Monitoring must be ingesting fresh node-exporter samples from both machines, otherwise every CPU / disk / network
    # metric of the run would silently come back empty (seen after a clock jump made Prometheus reject all samples).
    fresh = ch.by_machine(ch.prom('(time() - timestamp(up{job="node"})) and (up{job="node"} == 1)', time.time()))
    stale = [m for m in ch.MACHINES if fresh.get(m, 1e9) > 10]
    if stale:
        raise RuntimeError(f"monitoring not healthy: no fresh node-exporter samples from {stale} "
                           f"(check http://127.0.0.1:9090/targets)")


def execute(size, app, cfg, rep, rid) -> dict:
    src, out = dataset_path(size, cfg), f"{ch.HDFS}/tmp/runner/{rid}"
    confs = spark_confs(cfg)
    hdfs_rm(out)
    in_bytes = hdfs_bytes(src)
    ch.drop_caches()
    time.sleep(2)   # let the flush settle
    app_args = ["--k", "1024"] if app == "CpuBoundKMeans" else []
    cmd = ["docker", "exec", ch.CLIENT, "spark-submit", "--class", f"compbench.apps.{app}",
           *[x for kv in confs for x in ("--conf", kv)], "/jars/scala-2.13/compbench.jar",
           "--input", src, "--output", out, "--run-id", rid, *app_args]
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True)
    t1 = time.time()
    if r.returncode:
        raise RuntimeError(f"spark-submit failed:\n{(r.stdout + r.stderr)[-2500:]}")
    app_id = re.search(r"app-\d{14}-\d{4}", r.stdout + r.stderr).group(0)
    time.sleep(4)   # Prometheus scrapes every 2 s: wait for the samples covering the end of the run
    dur = t1 - t0
    pc_tx = ch.counter_delta('node_network_transmit_bytes_total{device="enp7s0",machine="pc"}', t1 + 4, dur).get("pc", 0)
    pc_rx = ch.counter_delta('node_network_receive_bytes_total{device="enp7s0",machine="pc"}', t1 + 4, dur).get("pc", 0)
    res = {
        "run_id": rid, "size_gb": float(size), "app": app, "config": cfg.name, "group": cfg.group,
        "storage": cfg.storage, "shuffle": cfg.shuffle, "rep": rep, "confs": confs, "app_id": app_id,
        "t0": t0, "t1": t1, "wall_s": round(dur, 2),
        "cpu_host": ch.cpu_stats(t0, t1 + 4),
        "net_pc_lan_tx_mb": round(pc_tx / 1e6, 1), "net_pc_lan_rx_mb": round(pc_rx / 1e6, 1),
        "disk_read_mb": {m: round(v / 1e6, 1) for m, v in ch.counter_delta('node_disk_read_bytes_total', t1 + 4, dur).items()},
        "disk_write_mb": {m: round(v / 1e6, 1) for m, v in ch.counter_delta('node_disk_written_bytes_total', t1 + 4, dur).items()},
        "eventlog": ch.eventlog_totals(app_id), "stages": eventlog_stages(app_id),
        "hdfs_input_bytes": in_bytes, "hdfs_output_bytes": hdfs_bytes(out),
    }
    res["link_airtime_pct"] = round(100 * (res["net_pc_lan_rx_mb"] * 8 / dur / ch.UPLINK_MBIT
                                           + res["net_pc_lan_tx_mb"] * 8 / dur / ch.DOWNLINK_MBIT), 1)
    hdfs_rm(out)
    return res


def git_commit() -> str:
    return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()


def netcheck(size: str) -> None:
    r = subprocess.run([str(ROOT / "deploy/scripts/netcheck.sh")], capture_output=True, text=True)
    d = RAW / "_netcheck"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{size}gb_{time.strftime('%Y%m%d-%H%M%S')}.json").write_text(r.stdout.strip() or json.dumps({"error": r.stderr[-300:]}))
    print(f"    netcheck: {r.stdout.strip() or r.stderr.strip()[-200:]}", flush=True)


# ---------------------------------------------------------------------------------------------- main

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sizes", nargs="+", default=SIZES_DEFAULT)
    ap.add_argument("--apps", nargs="+", default=list(ch.APPS), choices=list(ch.APPS))
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--configs", help="regex filter on config names, e.g. 'C0|C1'")
    ap.add_argument("--seed", default="compbench")
    ap.add_argument("--attempts", type=int, default=2, help="tries per run before it is recorded as failed")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    runs = plan(a.sizes, a.apps, a.reps, a.configs, a.seed)
    todo = [r for r in runs if not (RAW / run_id(r[0], r[1], r[2], r[3]) / "result.json").exists()]
    est = sum(float(s) * SEC_PER_GB[app] + OVERHEAD_S for s, app, _, _ in todo)
    print(f"{len(runs)} runs planned, {len(runs) - len(todo)} already done, {len(todo)} to run; "
          f"estimated {est / 3600:.1f} h", flush=True)
    if a.dry_run:
        by = {}
        for s, app, c, _ in todo:
            by[(s, app)] = by.get((s, app), 0) + 1
        for (s, app), n in sorted(by.items(), key=lambda kv: (float(kv[0][0]), kv[0][1])):
            print(f"  {s:>4} GB  {app:<20} {n:3d} runs")
        return 0

    assert_cluster()
    caff = subprocess.Popen(["ssh", "cb-mac-host", "caffeinate -dimsu -t 172800"], stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)   # keep the Mac awake
    done = failed = 0
    last_size = None
    try:
        for i, (size, app, cfg, rep) in enumerate(todo, 1):
            if size != last_size:
                print(f"\n=== dataset {size} GB ===", flush=True)
                netcheck(size)
                last_size = size
            rid = run_id(size, app, cfg, rep)
            d = RAW / rid
            d.mkdir(parents=True, exist_ok=True)
            for attempt in range(1, a.attempts + 1):
                try:
                    assert_cluster()
                    res = execute(size, app, cfg, rep, rid)
                    res["git_commit"], res["attempt"] = git_commit(), attempt
                    (d / "result.json").write_text(json.dumps(res, indent=1))
                    done += 1
                    print(f"[{i}/{len(todo)}] {rid}: {res['wall_s']:.0f}s  CPU(exec) "
                          f"{sum(h['cpu_s'] for h in res['eventlog']['by_host'].values()):.0f}s  "
                          f"shuffle {res['eventlog']['shuf_w_b'] / 1e9:.2f}GB  airtime {res['link_airtime_pct']}%", flush=True)
                    break
                except Exception as e:  # record and move on; the run is retried on the next invocation
                    (d / f"error_attempt{attempt}.txt").write_text(str(e)[-3000:])
                    print(f"[{i}/{len(todo)}] {rid}: attempt {attempt} FAILED: {str(e).splitlines()[0][:150]}", flush=True)
                    time.sleep(20)
            else:
                failed += 1
    finally:
        caff.terminate()
        subprocess.run(["ssh", "cb-mac-host", "pkill caffeinate"], capture_output=True)
    print(f"\ndone: {done} ok, {failed} failed", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
