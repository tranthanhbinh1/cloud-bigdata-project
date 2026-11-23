#!/usr/bin/env python3
"""Aggregate the campaign: results/raw/*/result.json -> results/summary/*.csv

  runs.csv      one row per run (flat metrics)
  cells.csv     one row per (size, app, config): n, median / min / max / mean / CV of the key metrics
  ratios.csv    compression ratios: storage (Parquet bytes on HDFS) and shuffle (bytes written by map tasks)
  outliers.csv  runs > 25 % and > 10 s slower than the median of their cell

Stdlib only.  python3 harness/collect.py
"""
from __future__ import annotations

import csv
import glob
import json
import statistics as st
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "results" / "raw"
OUT = ROOT / "results" / "summary"

APP_SHORT = {"IoBoundEtl": "A1", "CpuBoundKMeans": "A2", "BalancedRouteStats": "A3"}
APP_KIND = {"IoBoundEtl": "I/O-bound", "CpuBoundKMeans": "CPU-bound", "BalancedRouteStats": "balanced"}


def flatten(d: dict) -> dict:
    ev, cpu = d["eventlog"], d["cpu_host"]
    by = ev.get("by_host", {})
    return {
        "run_id": d["run_id"], "size_gb": d["size_gb"], "app": d["app"], "app_short": APP_SHORT[d["app"]],
        "kind": APP_KIND[d["app"]], "config": d["config"], "group": d["group"], "storage": d["storage"],
        "shuffle": d["shuffle"], "rep": d["rep"], "t0": round(d["t0"], 1), "wall_s": d["wall_s"],
        # CPU: executor CPU time from the event log (task threads) + host busy cores from node-exporter
        "exec_cpu_s": round(ev["cpu_ns"] / 1e9, 1),
        "exec_cpu_pc_s": round(by.get("cb-pc", {}).get("cpu_s", 0), 1),
        "exec_cpu_mac_s": round(by.get("cb-mac", {}).get("cpu_s", 0), 1),
        "task_run_s": round(ev["run_ms"] / 1e3, 1), "gc_s": round(ev["gc_ms"] / 1e3, 1),
        "host_cores_pc": cpu.get("pc", {}).get("mean_busy_cores"),
        "host_cores_mac": cpu.get("mac", {}).get("mean_busy_cores"),
        # disk (node-exporter, both machines; the Mac figure is the Docker Desktop VM's virtual disk)
        "disk_read_mb": round(sum(d["disk_read_mb"].values()), 1),
        "disk_write_mb": round(sum(d["disk_write_mb"].values()), 1),
        "disk_total_mb": round(sum(d["disk_read_mb"].values()) + sum(d["disk_write_mb"].values()), 1),
        # network: the PC's LAN interface carries all PC<->Mac traffic (Mac VM counters are unreliable)
        "net_tx_mb": d["net_pc_lan_tx_mb"], "net_rx_mb": d["net_pc_lan_rx_mb"],
        "net_total_mb": round(d["net_pc_lan_tx_mb"] + d["net_pc_lan_rx_mb"], 1),
        "link_airtime_pct": d["link_airtime_pct"],
        # Spark-level bytes
        "input_mb": round(d["hdfs_input_bytes"] / 1e6, 1), "output_mb": round(d["hdfs_output_bytes"] / 1e6, 1),
        "shuffle_write_mb": round(ev["shuf_w_b"] / 1e6, 1),
        "shuffle_remote_read_mb": round(ev["shuf_r_remote_b"] / 1e6, 1),
        "fetch_wait_s": round(ev["fetch_wait_ms"] / 1e3, 1), "spill_mb": round(ev["spill_b"] / 1e6, 1),
        "shuffle_stage_s": d["stages"].get("shuffle_map_stage_wall_s"),
        "attempt": d.get("attempt", 1),
    }


METRICS = ["wall_s", "exec_cpu_s", "disk_total_mb", "disk_read_mb", "disk_write_mb", "net_total_mb",
           "link_airtime_pct", "shuffle_write_mb", "input_mb", "output_mb", "host_cores_pc", "host_cores_mac"]


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"  {path.relative_to(ROOT)}: {len(rows)} rows")


def main() -> None:
    runs = [flatten(json.load(open(f))) for f in sorted(glob.glob(str(RAW / "*gb_*" / "result.json")))]
    OUT.mkdir(parents=True, exist_ok=True)
    runs.sort(key=lambda r: (r["size_gb"], r["app"], r["config"], r["rep"]))
    write_csv(OUT / "runs.csv", runs)

    cells = defaultdict(list)
    for r in runs:
        cells[(r["size_gb"], r["app"], r["config"])].append(r)

    cell_rows, outliers = [], []
    for (size, app, cfg), rs in sorted(cells.items()):
        row = {k: rs[0][k] for k in ("size_gb", "app", "app_short", "kind", "config", "group", "storage", "shuffle")}
        row["n"] = len(rs)
        for m in METRICS:
            v = [r[m] for r in rs if r[m] is not None]
            row[f"{m}_med"] = round(st.median(v), 2)
            row[f"{m}_min"] = round(min(v), 2)
            row[f"{m}_max"] = round(max(v), 2)
            row[f"{m}_mean"] = round(st.mean(v), 2)
            row[f"{m}_cv"] = round(100 * st.stdev(v) / st.mean(v), 2) if len(v) > 1 and st.mean(v) else 0
        cell_rows.append(row)
        med = row["wall_s_med"]
        for r in rs:
            if r["wall_s"] > 1.25 * med and r["wall_s"] - med > 10:
                outliers.append({"run_id": r["run_id"], "wall_s": r["wall_s"], "cell_median_s": med,
                                 "excess_s": round(r["wall_s"] - med, 1)})
    # speed-up vs C0 of the same (size, app)
    c0 = {(c["size_gb"], c["app"]): c["wall_s_med"] for c in cell_rows if c["config"] == "C0"}
    for c in cell_rows:
        c["speedup_vs_c0"] = round(c0[(c["size_gb"], c["app"])] / c["wall_s_med"], 3)
    write_csv(OUT / "cells.csv", cell_rows)
    write_csv(OUT / "outliers.csv", outliers or [{"run_id": "", "wall_s": "", "cell_median_s": "", "excess_s": ""}])

    # compression ratios
    ratios = []
    unc = {c["size_gb"]: c["input_mb_med"] for c in cell_rows if c["config"] == "C0" and c["app"] == "IoBoundEtl"}
    for c in cell_rows:
        if c["group"] == "C1" and c["app"] == "IoBoundEtl":   # input bytes depend only on the dataset
            ratios.append({"level": "storage", "app": "-", "size_gb": c["size_gb"], "codec": c["storage"],
                           "uncompressed_mb": unc[c["size_gb"]], "compressed_mb": c["input_mb_med"],
                           "ratio": round(unc[c["size_gb"]] / c["input_mb_med"], 3)})
    shuf0 = {(c["size_gb"], c["app"]): c["shuffle_write_mb_med"] for c in cell_rows if c["config"] == "C0"}
    for c in cell_rows:
        if c["group"] == "C2" and shuf0[(c["size_gb"], c["app"])] > 0:
            base = shuf0[(c["size_gb"], c["app"])]
            ratios.append({"level": "shuffle", "app": c["app_short"], "size_gb": c["size_gb"], "codec": c["shuffle"],
                           "uncompressed_mb": base, "compressed_mb": c["shuffle_write_mb_med"],
                           "ratio": round(base / c["shuffle_write_mb_med"], 3)})
    write_csv(OUT / "ratios.csv", ratios)

    print(f"{len(runs)} runs, {len(cell_rows)} cells, {len(outliers)} outlier runs, "
          f"median CV of wall time {st.median(c['wall_s_cv'] for c in cell_rows):.1f} %")


if __name__ == "__main__":
    main()
