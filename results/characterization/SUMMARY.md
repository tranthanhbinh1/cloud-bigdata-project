# Characterization gate — 5 GB, config C0 (no compression at either level)

Measured with `harness/characterize.py` (caches dropped on both machines first; Spark event log + node-exporter).

## CURRENT LAYOUT — 4 PC workers + 1 Mac worker (16 + 4 cores), raw LAN (run `pc16`)

The PC now hosts four identical workers (4 cores / 6 GB worker, one 4-core / 5 GB executor each) and the Mac one, i.e.
5 identical executors / 20 cores. Spark standalone cannot size executors per worker, so a bigger PC is expressed as more
identical workers, not one big one. Adopted because it cut every app's runtime by ~30 % (decision rule agreed beforehand:
keep it only if the campaign estimate drops >= 20 % and the three profiles still hold).

| App | 4+4 cores (`lan1`) | **16+4 cores (`pc16`)** | Executor CPU (PC of 16 / Mac of 4) | Est. link airtime | Verdict |
|---|---|---|---|---|---|
| A1 `IoBoundEtl` | 241 s | **170 s (-30 %)** | 9 % / 11 % | 103 % (saturated) | I/O-bound |
| A2 `CpuBoundKMeans` k=1024 | 108 s | **77 s (-28 %)** | 67 % / 71 % @5 GB; 77 % / 82 % @10 GB | 13 % @5 GB, 18 % @10 GB | CPU-bound |
| A3 `BalancedRouteStats` | 133 s | **95 s (-29 %)** | 38 % / 44 % | 49 % | balanced |

* A1 improved although it is network-bound because more shuffle stays on the PC: LAN traffic fell from 11.0 GB to 8.3 GB
  (remote shuffle read 9.6 -> 6.0 GB); the link is still saturated.
* A2's CPU share is lower at 5 GB (67-71 %) than before (86-89 %) because of wave quantisation (62 tasks over 20 slots) and ~10 s of
  fixed Spark startup in a 77 s run; it recovers with size (77-82 % at 10 GB).
* Noise under this layout (A3, C0, 2.5 GB, 3 runs): wall 52.4 / 52.1 / 54.7 s = CV 2.7 %, executor CPU CV 0.5 %.
* HDFS blocks written by the executors skew toward the PC (it runs most tasks): ~13 vs 3 blocks in the health check.
* Raw: `5gb_*_pc16.json`, `10gb_CpuBoundKMeans_pc16.json`.

## Previous layout — 4 + 4 cores, raw LAN (run `lan1`)

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
