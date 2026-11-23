#!/usr/bin/env python3
"""Figures for the report: results/summary/*.csv + results/codec-bench/*.csv -> results/plots/*.png

Required by the project brief (fig1-fig7) plus extras (fig8+).  Every point / bar is the MEDIAN of the 3
repetitions; error bars span min..max.  Run harness/collect.py first.

    python3 harness/plots.py
"""
from __future__ import annotations

import csv
import glob
import statistics as st
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SUM, OUT = ROOT / "results" / "summary", ROOT / "results" / "plots"

# ---- palette (validated categorical slots; text in ink tokens, never series colours) ----
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
CODEC = {"snappy": "#2a78d6", "lz4": "#eb6834", "gzip": "#1baf7a", "zstd": "#eda100",
         "snappy+lz4": "#6250d6", "gzip+zstd": "#008300", "none": INK2}
MARK = {"snappy": "o", "lz4": "s", "gzip": "D", "zstd": "^", "snappy+lz4": "P", "gzip+zstd": "X", "none": "o"}
LEVEL = {"C0": "#9ec5f4", "C1": "#5598e7", "C2": "#256abf", "C3": "#104281"}   # ordinal blue ramp
APPS = [("IoBoundEtl", "A1 · I/O-bound"), ("CpuBoundKMeans", "A2 · CPU-bound"), ("BalancedRouteStats", "A3 · balanced")]
GROUPS = [("C1", "C1 · storage only"), ("C2", "C2 · shuffle only"), ("C3", "C3 · both")]

plt.rcParams.update({
    "font.size": 9, "axes.titlesize": 10, "axes.labelsize": 9, "axes.edgecolor": INK2, "axes.labelcolor": INK,
    "xtick.color": INK2, "ytick.color": INK2, "text.color": INK, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
    "legend.frameon": False, "figure.facecolor": "white", "savefig.facecolor": "white", "lines.linewidth": 2,
})


def load(name: str) -> list[dict]:
    rows = list(csv.DictReader(open(SUM / name)))
    for r in rows:
        for k, v in r.items():
            try:
                r[k] = float(v)
            except (TypeError, ValueError):
                pass
    return rows


def label_of(cfg: dict) -> str:
    return {"C0": "none", "C1": cfg["storage"], "C2": cfg["shuffle"]}.get(cfg["group"], f"{cfg['storage']}+{cfg['shuffle']}")


def save(fig, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / name, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  results/plots/{name}")


def errbar(ax, x, med, lo, hi, color, marker, label, ls="-"):
    ax.errorbar(x, med, yerr=[[m - l for m, l in zip(med, lo)], [h - m for m, h in zip(med, hi)]], color=color,
                marker=marker, markersize=5, ls=ls, capsize=2.5, elinewidth=1, label=label)


# ------------------------------------------------------------------------------------------- figures 1-4

def vs_size(cells, metric: str, ylabel: str, scale: float, fname: str, title: str) -> None:
    """Small multiples: rows = compression group, cols = app; each panel = C0 (grey) + that group's codecs."""
    fig, axes = plt.subplots(3, 3, figsize=(11, 9), sharex=True)
    sizes = sorted({c["size_gb"] for c in cells})
    for j, (app, app_t) in enumerate(APPS):
        mine = [c for c in cells if c["app"] == app]
        for i, (grp, grp_t) in enumerate(GROUPS):
            ax = axes[i][j]
            sel = [c for c in mine if c["group"] in ("C0", grp)]
            if not any(c["group"] == grp for c in sel):
                ax.set_axis_off()
                ax.text(0.5, 0.5, f"{app_t.split(' · ')[0]} has no shuffle:\nonly C0 and C1 are run",
                        ha="center", va="center", color=INK2, transform=ax.transAxes)
                continue
            series = defaultdict(list)
            for c in sel:
                series[label_of(c)].append(c)
            for lab in sorted(series, key=lambda s: (s != "none", list(CODEC).index(s))):
                pts = sorted(series[lab], key=lambda c: c["size_gb"])
                errbar(ax, [p["size_gb"] for p in pts], [p[f"{metric}_med"] * scale for p in pts],
                       [p[f"{metric}_min"] * scale for p in pts], [p[f"{metric}_max"] * scale for p in pts],
                       CODEC[lab], MARK[lab], "C0 (none)" if lab == "none" else lab, ls="--" if lab == "none" else "-")
            ax.set_xticks(sizes)
            ax.set_xticklabels([f"{s:g}" for s in sizes])
            ax.set_ylim(bottom=0)
            if i == 0:
                ax.set_title(app_t, fontweight="bold")
            if j == 0:
                ax.set_ylabel(f"{grp_t}\n{ylabel}")
            if i == 2 or (j == 1 and i == 0):
                ax.set_xlabel("dataset size (GB, uncompressed Parquet)")
            ax.legend(fontsize=7.5, loc="upper left")
    fig.suptitle(title, x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.text(0.01, 0.945, "median of 3 runs; bars = min..max", color=INK2, fontsize=8.5)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    save(fig, fname)


# ------------------------------------------------------------------------------------------- figure 5

def e0_rows() -> list[dict]:
    rows = []
    for f in glob.glob(str(ROOT / "results" / "codec-bench" / "*.csv")):
        sample = Path(f).stem.split("_", 1)[1]
        for r in csv.DictReader(open(f)):
            r["sample"] = sample
            rows.append(r)
    return rows


def e0_agg(rows):
    """median per (machine, sample, codec-level)"""
    g = defaultdict(list)
    for r in rows:
        g[(r["machine"], r["sample"], r["codec"], r["level"])].append(r)
    out = {}
    for k, rs in g.items():
        out[k] = {m: st.median(float(r[m]) for r in rs)
                  for m in ("ratio", "comp_mb_s", "decomp_mb_s", "comp_cpu_s_per_gb", "decomp_cpu_s_per_gb")}
    return out


def fig5_ratio(ratios, e0) -> None:
    big = max(r["size_gb"] for r in ratios)
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8), gridspec_kw={"width_ratios": [4, 6, 7]})
    # (a) storage: Parquet on HDFS
    ax = axes[0]
    st_r = {r["codec"]: r["ratio"] for r in ratios if r["level"] == "storage" and r["size_gb"] == big}
    codecs = ["snappy", "lz4", "gzip", "zstd"]
    ax.bar(codecs, [st_r[c] for c in codecs], color=[CODEC[c] for c in codecs], width=0.65)
    for k, c in enumerate(codecs):
        ax.text(k, st_r[c] + 0.04, f"{st_r[c]:.2f}×", ha="center", fontsize=8)
    ax.set_title(f"(a) storage: Parquet on HDFS ({big:g} GB)")
    ax.set_ylabel("compression ratio (uncompressed / compressed)")
    # (b) shuffle: bytes written by map tasks, C0 vs C2
    ax = axes[1]
    w = 0.38
    for a_i, (app, lab) in enumerate([("A1", "A1 · I/O-bound"), ("A3", "A3 · balanced")]):
        sh = {r["codec"]: r["ratio"] for r in ratios if r["level"] == "shuffle" and r["app"] == app and r["size_gb"] == big}
        cs = ["lz4", "snappy", "zstd"]
        xs = [k + (a_i - 0.5) * w for k in range(len(cs))]
        ax.bar(xs, [sh[c] for c in cs], width=w - 0.04, color=[CODEC[c] for c in cs],
               hatch=None if a_i == 0 else "////", edgecolor="white", linewidth=0)
        for x, c in zip(xs, cs):
            ax.text(x, sh[c] + 0.06, f"{sh[c]:.1f}×", ha="center", fontsize=7.5)
    ax.set_xticks(range(3))
    ax.set_xticklabels(["lz4", "snappy", "zstd"])
    ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, fc=INK2), plt.Rectangle((0, 0), 1, 1, fc=INK2, hatch="////", ec="white")],
              labels=["A1 (solid)", "A3 (hatched)"], fontsize=7.5, loc="upper left")
    ax.set_title(f"(b) shuffle: map output bytes, C0 / C2 ({big:g} GB)")
    # (c) intrinsic (E0): single-thread codec bench on the PC, Parquet page bytes vs CSV text
    ax = axes[2]
    if e0:
        levels = [("snappy", "-"), ("lz4", "-"), ("gzip", "1"), ("gzip", "6"), ("gzip", "9"), ("zstd", "1"), ("zstd", "3"), ("zstd", "9")]
        for s_i, sample in enumerate(["parquet", "csv"]):
            xs = [k + (s_i - 0.5) * w for k in range(len(levels))]
            vals = [e0.get(("pc", sample, c, l), {}).get("ratio", 0) for c, l in levels]
            ax.bar(xs, vals, width=w - 0.04, color=[CODEC[c] for c, _ in levels],
                   hatch=None if s_i == 0 else "////", edgecolor="white", linewidth=0)
        ax.set_xticks(range(len(levels)))
        ax.set_xticklabels([c if l == "-" else f"{c}-{l}" for c, l in levels], rotation=30, ha="right")
        ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, fc=INK2), plt.Rectangle((0, 0), 1, 1, fc=INK2, hatch="////", ec="white")],
                  labels=["uncompressed Parquet file (solid)", "same rows as CSV text (hatched)"], fontsize=7.5, loc="upper left")
        ax.set_title("(c) intrinsic (E0): standalone codec benchmark")
    for ax in axes:
        ax.grid(axis="x", visible=False)
        ax.set_ylim(0, ax.get_ylim()[1] * 1.12)
    fig.suptitle("Compression ratio by algorithm", x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    save(fig, "fig5_ratio_vs_algorithm.png")


# ------------------------------------------------------------------------------------------- figure 6

ORDER = ["C0", "C1-snappy", "C1-lz4", "C1-gzip", "C1-zstd", "C2-lz4", "C2-snappy", "C2-zstd", "C3-snappy+lz4", "C3-gzip+zstd"]


def fig6_configs(cells, size: float) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.2), gridspec_kw={"width_ratios": [10, 5.5, 10]})
    for ax, (app, app_t) in zip(axes, APPS):
        mine = {c["config"]: c for c in cells if c["app"] == app and c["size_gb"] == size}
        cfgs = [c for c in ORDER if c in mine]
        xs, x, prev = [], 0.0, None
        for c in cfgs:                       # a gap between the C-levels
            g = c.split("-")[0]
            x += 0.6 if prev and g != prev else 0
            xs.append(x)
            x += 1
            prev = g
        med = [mine[c]["wall_s_med"] for c in cfgs]
        lo = [mine[c]["wall_s_min"] for c in cfgs]
        hi = [mine[c]["wall_s_max"] for c in cfgs]
        cols = [CODEC[label_of(mine[c])] for c in cfgs]
        ax.bar(xs, med, color=cols, width=0.8)
        ax.errorbar(xs, med, yerr=[[m - l for m, l in zip(med, lo)], [h - m for m, h in zip(med, hi)]],
                    fmt="none", ecolor=INK, elinewidth=1, capsize=2.5)
        ax.axhline(mine["C0"]["wall_s_med"], color=INK2, ls="--", lw=1)
        for xx, c, h in zip(xs, cfgs, hi):
            if c != "C0":
                ax.text(xx, h + max(med) * 0.02, f"{mine[c]['speedup_vs_c0']:.2f}×", ha="center", va="bottom",
                        rotation=90, fontsize=7, color=INK2)
        ax.set_xticks(xs)
        ax.set_xticklabels([label_of(mine[c]) if c != "C0" else "C0" for c in cfgs], rotation=40, ha="right", fontsize=8)
        # group captions under the axis
        for g in ("C1", "C2", "C3"):
            gx = [xx for xx, c in zip(xs, cfgs) if c.startswith(g)]
            if gx:
                ax.text(sum(gx) / len(gx), -0.30, {"C1": "storage", "C2": "shuffle", "C3": "both"}[g],
                        transform=ax.get_xaxis_transform(), ha="center", fontsize=8, color=INK2, fontweight="bold")
        ax.set_title(app_t, fontweight="bold")
        ax.grid(axis="x", visible=False)
        ax.set_ylim(0, max(hi) * 1.22)
    axes[0].set_ylabel("execution time (s)")
    fig.suptitle(f"Execution time by compression configuration ({size:g} GB)", x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.text(0.01, 0.905, "median of 3 runs, bars = min..max; dashed line = C0; label = speed-up vs C0", color=INK2, fontsize=8.5)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    save(fig, f"fig6_time_vs_config_{size:g}gb.png")


# ------------------------------------------------------------------------------------------- figure 7

def fig7_workload(cells) -> None:
    sizes = sorted({c["size_gb"] for c in cells})
    fig, axes = plt.subplots(1, len(sizes), figsize=(13, 3.9), sharey=True)
    w = 0.2
    for ax, size in zip(axes, sizes):
        for k, (app, app_t) in enumerate(APPS):
            mine = [c for c in cells if c["app"] == app and c["size_gb"] == size]
            c0 = next(c for c in mine if c["config"] == "C0")["wall_s_med"]
            for g_i, g in enumerate(["C0", "C1", "C2", "C3"]):
                grp = [c for c in mine if c["group"] == g]
                if not grp:
                    continue
                best = min(grp, key=lambda c: c["wall_s_med"])
                x = k + (g_i - 1.5) * w
                ax.bar(x, best["wall_s_med"] / c0, width=w - 0.03, color=LEVEL[g])
                ax.errorbar(x, best["wall_s_med"] / c0, yerr=[[(best["wall_s_med"] - best["wall_s_min"]) / c0],
                                                              [(best["wall_s_max"] - best["wall_s_med"]) / c0]],
                            fmt="none", ecolor=INK, elinewidth=0.9, capsize=2)
                if g != "C0":
                    ax.text(x, best["wall_s_max"] / c0 + 0.02, label_of(best), rotation=90, ha="center", va="bottom",
                            fontsize=6.5, color=INK2)
        ax.axhline(1, color=INK2, ls="--", lw=1)
        ax.set_xticks(range(3))
        ax.set_xticklabels([t.replace(" · ", "\n") for _, t in APPS], fontsize=8)
        ax.set_title(f"{size:g} GB", fontweight="bold")
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("time relative to C0 (lower = faster)")
    axes[0].set_ylim(0, 1.9)
    axes[-1].legend(handles=[plt.Rectangle((0, 0), 1, 1, fc=LEVEL[g]) for g in LEVEL],
                    labels=["C0 none", "C1 storage (best codec)", "C2 shuffle (best codec)", "C3 both (best pair)"],
                    fontsize=7.5, loc="upper right")
    fig.suptitle("Effect of compression by workload type", x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.text(0.01, 0.9, "best configuration of each level, normalized to the C0 median of the same app and size; "
             "label = winning codec", color=INK2, fontsize=8.5)
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    save(fig, "fig7_time_vs_workload.png")


# ------------------------------------------------------------------------------------------- extras

def fig8_e0_speed(e0) -> None:
    if not e0:
        return
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    for ax, mode, lab in [(axes[0], "comp_mb_s", "compression"), (axes[1], "decomp_mb_s", "decompression")]:
        for (machine, sample, codec, level), v in sorted(e0.items()):
            if sample != "parquet":
                continue
            ax.scatter(v[mode], v["ratio"], color=CODEC[codec], marker="o" if machine == "pc" else "^", s=40,
                       edgecolor="white", linewidth=1, zorder=3)
            if machine == "pc":
                name = codec if level == "-" else f"{codec}-{level}"
                off = {"snappy": (-8, -13), "lz4": (5, 5), "gzip-9": (5, -12), "gzip-6": (5, 5)}.get(name, (5, 3))
                if mode == "decomp_mb_s":            # gzip-6 and gzip-9 decompress at the same speed: one label
                    if name == "gzip-9":
                        continue
                    name = "gzip-6 / gzip-9" if name == "gzip-6" else name
                    off = (5, -12) if name == "gzip-1" else off
                ax.annotate(name, (v[mode], v["ratio"]), textcoords="offset points", xytext=off, fontsize=7, color=INK2)
        ax.set_xscale("log")
        ax.set_xlabel(f"{lab} throughput, single thread (MB/s, log scale)")
        ax.set_title(lab.capitalize(), fontweight="bold")
    axes[0].set_ylabel("compression ratio")
    axes[0].set_ylim(1.33, 2.32)
    axes[1].legend(handles=[Line2D([], [], marker="o", ls="", color=INK2, label="PC (i5-13500, P-core)"),
                            Line2D([], [], marker="^", ls="", color=INK2, label="Mac (M1 Pro, in Docker VM)")],
                   fontsize=7.5, loc="lower left")
    fig.suptitle("E0 · codec speed vs ratio (uncompressed Parquet sample)", x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    save(fig, "fig8_e0_speed_vs_ratio.png")


def fig9_stalls(runs, cells) -> None:
    med = {(c["size_gb"], c["app"], c["config"]): c["wall_s_med"] for c in cells}
    fig, ax = plt.subplots(figsize=(11, 3.6))
    runs = sorted(runs, key=lambda r: r["t0"])
    t0 = runs[0]["t0"]
    xs = [(r["t0"] - t0) / 3600 for r in runs]
    ys = [r["wall_s"] / med[(r["size_gb"], r["app"], r["config"])] for r in runs]
    cols = [CODEC["lz4"] if y > 1.25 else CODEC["snappy"] for y in ys]
    ax.scatter(xs, ys, c=cols, s=14, zorder=3)
    ax.axhline(1.25, color=INK2, ls="--", lw=1)
    ax.text(xs[-1], 1.29, "outlier threshold (+25 %)", ha="right", fontsize=7.5, color=INK2)
    k = 0
    for x, y, r in zip(xs, ys, runs):
        if y > 1.25:   # alternate above / below so neighbouring outliers do not collide
            ax.annotate(r["run_id"], (x, y), textcoords="offset points", xytext=(6, 6 if k % 2 == 0 else -10),
                        fontsize=6.5, color=INK2)
            k += 1
    ax.set_xlabel("hours since campaign start")
    ax.set_ylabel("run time / median of its 3 reps")
    fig.suptitle("Run-to-run stability over the campaign (300 runs)", x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    save(fig, "fig9_stability.png")


def main() -> None:
    cells, runs, ratios = load("cells.csv"), load("runs.csv"), load("ratios.csv")
    e0 = e0_agg(e0_rows())
    vs_size(cells, "wall_s", "execution time (s)", 1, "fig1_time_vs_size.png", "Execution time vs dataset size")
    vs_size(cells, "exec_cpu_s", "executor CPU time (s)", 1, "fig2_cpu_vs_size.png", "CPU consumption vs dataset size")
    vs_size(cells, "disk_total_mb", "disk read + write (GB)", 1e-3, "fig3_disk_vs_size.png", "Disk I/O vs dataset size (both machines)")
    vs_size(cells, "net_total_mb", "PC↔Mac traffic (GB)", 1e-3, "fig4_network_vs_size.png", "Network traffic vs dataset size")
    fig5_ratio(ratios, e0)
    for size in sorted({c["size_gb"] for c in cells}):
        fig6_configs(cells, size)
    fig7_workload(cells)
    fig8_e0_speed(e0)
    fig9_stalls(runs, cells)


if __name__ == "__main__":
    main()
