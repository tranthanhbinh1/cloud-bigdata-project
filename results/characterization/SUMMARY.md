# Characterization gate — 5 GB, config C0 (no compression at either level)

Measured with `harness/characterize.py` (caches dropped on both machines first; Spark event log + node-exporter).
Cluster: PC worker + Mac worker, 4 cores / 6 GB each.

## FINAL profiles — raw LAN (current setup, run `lan1`)

Link: PC<->Mac over Wi-Fi/LAN with no overlay. Measured capacity (iperf3, 4 streams): Mac->PC ~280 Mbit/s,
PC->Mac ~535 Mbit/s. Wi-Fi is half-duplex, so "airtime" = Mac->PC rate / 280 + PC->Mac rate / 535 (estimate).
Network bytes come from the PC's `enp7s0` only (it sees every byte crossing the LAN; the Mac VM's counters are
unreliable under Docker Desktop host networking).

| App | Wall | Executor CPU (of 4 cores) | Shuffle written | LAN traffic (Mac->PC / PC->Mac) | Est. link airtime | Verdict |
|---|---|---|---|---|---|---|
| A1 `IoBoundEtl` | 241 s | 11 % (Mac) / 14 % (PC) | 22.5 GB | 173 / 192 Mbit/s | **97.5 %** | **I/O-bound** |
| A2 `CpuBoundKMeans` (k=1024) | 108 s | 86 % / 89 % | 0 | 1 / 2 Mbit/s | **0.8 %** | **CPU-bound** |
| A3 `BalancedRouteStats` | 133 s | 55 % / 60 % | 2.1 GB | 66 / 65 Mbit/s | **35.7 %** | **balanced** (CPU-leaning) |

Raw: `5gb_*_lan1.json`.

> Everything below is the earlier Tailscale-era gate, kept as history (the overlay was dropped: its throughput
> swung between 38 and 285 Mbit/s while raw LAN stayed steady). The tuning table explains how A3 got its shape.

# Earlier gate over Tailscale (superseded)

Link then: Wi-Fi via Tailscale (iperf3 220-285 Mbit/s when it behaved). "Network" = `tailscale0` traffic.

## Profiles (Tailscale-era)

| App | Wall | Executor CPU (of 4 cores) | Shuffle written | Network | Disk (host, written) | Verdict |
|---|---|---|---|---|---|---|
| A1 `IoBoundEtl` | 315 s | 10–12 % | 22.5 GB | 11.9 GB, ~300 Mbit/s (link saturated) | 31 GB PC / 16 GB Mac | **I/O-bound** |
| A2 `CpuBoundKMeans` (k=1024) | 111 s | 87–88 % | 0 | ~40 MB (~1 %) | <0.1 GB | **CPU-bound** |
| A3 `BalancedRouteStats` | 146 s | 54–57 % | 2.1 GB | 2.3 GB, ~126 Mbit/s (~50 % of link) | 1.3 GB PC / 0.8 GB Mac | **balanced** |

Raw: `5gb_IoBoundEtl_gate1.json`, `5gb_CpuBoundKMeans_gate1-k1024.json`, `5gb_BalancedRouteStats_gate5.json`.

## Tuning history (why the apps look the way they do)

* **A2:** k=512 gave 80–82 % executor CPU (borderline); k=1024 gives 87–88 % -> default k=1024. Reads only 1.3 GB of the 5.2 GB (8 of 18 columns).
* **A3** went through four versions; the link (~35 MB/s) makes any whole-row shuffle network-bound:

| Version | Shuffled | CPU | Network | Result |
|---|---|---|---|---|
| join stats back onto all trips (+ percentile_approx) | 12.8 GB | 25–39 % | saturated (265 Mbit/s) | I/O-bound, not balanced |
| join onto a narrow projection | 8.6 GB | 18–24 % | saturated | still network-bound |
| groupBy (pu, do, hour), rich aggregates, no join | 6.5 GB | 35–36 % | saturated | partial-aggregation buffers too large (1.6M groups) |
| groupBy (pu, do) | 0.84 GB | 62–63 % | ~20 % | CPU-leaning: shuffle compression would barely matter |
| **groupBy (pu, do, 6-hour day part)** | **2.1 GB** | **54–57 %** | **~50 %** | **balanced (final)** |

## Observations worth carrying into the report

* Spark's shuffle format is far larger than columnar Parquet: A1 shuffles 22.5 GB from a 5.2 GB input (4.3x).
* Network counters are self-consistent: PC tx ≈ Mac rx within ~2 % in every run.
* Task locality was ~100 % local in these runs, but HDFS blocks are only ~55/45 PC/Mac, so per-run locality is recorded.
