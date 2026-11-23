# Analysis: answers to Q1–Q7 (draft)

Evidence base: 300 runs (4 sizes × 25 app-configurations × 3 repetitions, 0 failures) plus the E0 codec benchmark.
Unless noted, a number is the **median of 3 runs**; "significant" means the min..max ranges of the two
configurations do not overlap. Speed-up = C0 median time / configuration median time (> 1 = faster).
Tables: `results/summary/cells.csv`, `ratios.csv`; figures: `results/plots/fig1..fig9`.

Codec settings: storage = Parquet codec (gzip level 6, zstd level 3, Parquet `lz4` = Hadoop LZ4);
shuffle = `spark.io.compression.codec` (zstd level 1, Spark's default; gzip is not available for the shuffle).

Cluster context that drives most results: 20 executor cores (16 on the PC, 4 on the Mac); the PC↔Mac link is
Wi-Fi at ~280 Mbit/s (Mac→PC) and ~535 Mbit/s (PC→Mac); disks are local NVMe; 91 % of A1's tasks (and 89 % of A3's) ran node-local, i.e. read HDFS blocks from
their own machine.

---

## Q1. Which algorithms give the best trade-off between compression ratio and CPU overhead?

**zstd, at both levels.** It has the best ratio among the fast codecs and costs much less CPU than gzip. gzip is
dominated by zstd everywhere; snappy and lz4 are the cheapest but compress least.

| | snappy | lz4 | gzip | zstd |
|---|---|---|---|---|
| E0 ratio, Parquet sample | 1.43 | 1.43 | 1.81 (level 6) | 1.94 (level 3) |
| E0 compression speed, 1 core, PC (MB/s) | 802 | 825 | 26 | 274 |
| E0 compression CPU (s per GB) | 1.1 | 1.2 | 38.5 | 3.7 |
| Storage ratio on HDFS (10 GB) | 1.42 | 1.43 | 1.80 | 1.88 |
| A1 extra executor CPU vs C0, storage (10 GB) | +3 % | +2 % | **+62 %** | +5 % |
| Shuffle ratio, A1 (10 GB) | 3.1 | 2.8 | n/a | **4.8** |
| A1 extra executor CPU vs C0, shuffle (10 GB) | +4 % | +4 % | n/a | +26 % |
| A1 speed-up, shuffle only (10 GB) | 2.54× | 2.39× | n/a | **3.15×** |

- **zstd** gives the highest ratio at both levels and the best time at both levels. Its CPU cost (+5 % storage,
  +26 % shuffle on A1) is repaid many times by the traffic it removes.
- **gzip** is 10× slower than zstd-3 at compressing and compresses less (E0: 26 vs 274 MB/s, ratio 1.81 vs 1.94).
  Higher gzip levels gain almost nothing on Parquet: gzip-9 is 4× slower than gzip-6 for the same 1.81× (fig8).
- **snappy / lz4** are nearly free in CPU (+2–4 %) but save the least. They are the right choice only when CPU,
  not I/O, is scarce.
- The ratios measured inside Spark match E0 within 3 %. The intrinsic benchmark therefore predicts in-situ
  compression well; what it cannot predict is whether the saved bytes were on the critical path (Q2–Q5).

## Q2. Does compression systematically improve I/O-bound applications?

**No. Shuffle compression does, storage compression does not** (A1, figs 1, 6, 7).

- **Shuffle compression (C2): every codec at every size is significantly faster**, from 1.67× (lz4, 0.5 GB) to
  3.15× (zstd, 10 GB). A1 is limited by the Wi-Fi link: the link is ~100 % busy under C0 from 2.5 GB upward. Its
  shuffle is 4.3× its input (45 GB at 10 GB). Compression cuts PC↔Mac traffic from 23.9 GB to 6.6 GB (zstd), and
  the summed shuffle fetch-wait time from 7,396 s to 848 s.
- **Storage compression (C1) is not systematic:**

  | A1, storage only | 0.5 GB | 2.5 GB | 5 GB | 10 GB |
  |---|---|---|---|---|
  | snappy | 1.04 | **0.95** | 0.92 | 1.08 |
  | lz4 | 1.01 | **0.88** | 0.98 | 1.16 |
  | gzip | 1.01 | **0.94** | 0.98 | 1.17 |
  | zstd | 1.03 | **0.96** | 0.99 | 1.12 |

  (bold = significantly slower than C0)

  Storage compression only shrinks the input read, and 91 % of A1's tasks read from their own machine's NVMe disk, which
  is not A1's bottleneck. It does **not** shrink the shuffle (45 GB under every C1 codec), so the saturated link is
  untouched. It starts to pay off only at 10 GB (Q4).
- So "I/O-bound" is not enough to predict a benefit. Compression helps when it shrinks the bytes on the
  bottleneck resource.

## Q3. How does compression affect CPU-bound applications?

**It has no measurable benefit and a small cost** (A2, figs 2, 6, 7).

- Every A2 configuration is within ±5 % of C0 at every size. The only significant differences are slow-downs
  (4 of 16 cases): snappy storage is 5 % slower at 5 GB and 10 GB, gzip 4 % slower at 5 GB, and lz4 4 % slower
  at 2.5 GB.
- Executor CPU rises 0–5 %. That matches the E0 decompression costs: reading 10.5 GB adds 6 s (snappy) to 39 s
  (gzip) of CPU, against ~2,020 s of K-means CPU, so at most 2 %.
- Storage compression still cuts A2's input bytes (10.5 → 5.6 GB with zstd) and disk reads (3.7 → 2.6 GB). That
  saving does not translate into time because A2 keeps the PC's cores ~80 % busy (12.9 of 16 at 10 GB) and moves
  almost nothing over the network (link 13–19 % busy). There is no idle I/O time for compression to recover.
- A2 has no shuffle, so communication compression is not applicable (only C0 and C1 were run).

## Q4. How do the benefits evolve with increasing dataset size?

**They grow with size, for two reasons. The algorithm ranking does not change.**

| best speed-up vs C0 | 0.5 GB | 2.5 GB | 5 GB | 10 GB |
|---|---|---|---|---|
| A1, shuffle zstd (C2) | 2.02× | 3.00× | 2.71× | 3.15× |
| A1, gzip + zstd (C3) | 2.05× | 2.90× | 2.67× | **4.12×** |
| A3, shuffle zstd (C2) | 1.31× | 1.25× | 1.31× | 1.37× |
| A3, gzip + zstd (C3) | 1.25× | 1.23× | 1.26× | 1.41× |
| A3, storage zstd (C1) | 1.05× | 1.00× | 0.99× | **1.14×** |

1. **Fixed costs dilute the benefit at small sizes.** A 0.5 GB run includes ~10–15 s of Spark start-up and
   scheduling that compression cannot shorten. A1's C0 time is 68 s/GB at 0.5 GB against 38–51 s/GB above that.
2. **At 10 GB, I/O becomes the limit on the Mac as well, so storage compression starts paying off.** From 5 GB
   to 10 GB the data doubles, but the Mac's disk reads during A1/C0 grow 6× (4.6 → 28.1 GB) and its writes 2.7×.
   That is consistent with the Mac VM's 10 GB of memory no longer holding the working set (shuffle files and
   input), so data is re-read from its virtual disk instead of from memory. This interpretation is inferred from
   the counters, not measured directly. In that regime:
   - storage compression shortens A1's map stage from 85 s (C0) to 50 s (gzip);
   - A3's storage-only zstd goes from no effect to 1.14×;
   - combined compression (C3) pulls ahead of shuffle-only (C2) for A1: 123 s vs 161 s.
- **The relative order of the algorithms is stable.**
  - Ratios do not depend on size: storage 1.42 / 1.43 / 1.80 / 1.88 at every size; shuffle within ±2 %.
  - The shuffle ranking zstd > snappy > lz4 holds for A1 and A3 at all four sizes.
  - Among storage codecs, the differences below 10 GB are within run-to-run variation, so no ranking is claimed.
- Absolute savings grow linearly with size: A1's network traffic is 1.3 / 4.4 / 11.4 / 23.9 GB under C0, and
  zstd shuffle removes 72–82 % of it at every size.

## Q5. Is compression more beneficial for storage or for network communication?

**For network communication, by a wide margin** (figs 4, 5, 6).

| 10 GB | best storage only (C1) | best shuffle only (C2) |
|---|---|---|
| A1 | 1.17× (gzip) | **3.15×** (zstd) |
| A3 | 1.14× (zstd) | **1.37×** (zstd) |

C2 beats C1 at every size for both shuffle-heavy apps. Three measured reasons:

1. **Volume.** A1's shuffle (45 GB) is 4.3× its input (10.5 GB), so there are more bytes to save.
2. **Compressibility.** The same codec compresses Spark's row-format shuffle data 2.7–4.8× but columnar Parquet
   only 1.4–1.9×, because Parquet is already dictionary- and run-length-encoded. E0 shows the same gap: the same
   rows as CSV text compress 2.5–5.7× against 1.4–2.3× for the Parquet file (fig5c).
3. **Where the bottleneck is.** The shuffle crosses the Wi-Fi link, which runs at its ceiling: PC-side throughput
   stays at 40–55 MB/s whatever the codec. Compression does not raise that throughput, it removes bytes (23.9 →
   6.6 GB). Storage reads are mostly local NVMe reads, which were not limiting until 10 GB (Q4).

Shuffle compression was never counterproductive here: all 3 codecs × 2 apps × 4 sizes are significantly faster.
Caveat: this is a slow-network cluster. The shuffle advantage should shrink on a fast LAN, but we did not measure
that (see Q7).

## Q6. Can combining storage and communication compression degrade performance?

**Yes, mildly, when the storage half has nothing to save. When it does, the effects add up** (fig6, C3 vs C2).

| C3 time / C2 time with the same shuffle codec | 0.5 GB | 2.5 GB | 5 GB | 10 GB |
|---|---|---|---|---|
| A1: snappy+lz4 vs lz4 | 0.97 | **1.16** (slower) | 1.00 | 1.03 |
| A1: gzip+zstd vs zstd | 0.99 | 1.03 | 1.01 | **0.76** (faster) |
| A3: snappy+lz4 vs lz4 | 1.01 | 0.96 | 1.01 | **1.04** (slower) |
| A3: gzip+zstd vs zstd | 1.04 | 1.01 | **1.04** (slower) | 0.98 |

(bold = significant)

- **Below 10 GB, adding storage compression to shuffle compression gains nothing.** It is within ±4 % of
  shuffle-only, apart from one 16 % slow-down (A1 snappy+lz4 at 2.5 GB). The extra cost is CPU: the gzip+zstd
  pair needs 1.65–2.0× A1's C0 CPU, against 1.25× for zstd shuffle alone.
- **At 10 GB the two levels add up** for the heavy pair on A1: 4.12× vs 3.15× for shuffle-only. Storage
  compression then relieves the Mac's disk (Q4) while shuffle compression relieves the link.
- **The two mechanisms are independent.** Storage compression does not change the shuffle volume (A1 writes
  44.9 GB of shuffle data under C0 and every C1 codec). C3's gain is therefore at most the sum of the two separate
  gains, and the storage part is only positive when input I/O is a bottleneck.
- On a CPU-saturated cluster the doubled CPU of gzip+zstd would turn into a slow-down; A2 shows that pattern for
  storage compression alone (Q3).

## Q7. Under which conditions should compression be disabled?

From our measurements:

1. **CPU-bound jobs** (A2): no configuration was faster, and 4 of 16 size × codec cases were significantly
   slower (−4 to −5 %).
   Leave storage compression off when cores are the bottleneck, or use the cheapest codec if disk space matters.
2. **Storage compression on small or medium datasets whose input is read locally** (A1 at 0.5–5 GB): no gain,
   and all four codecs were significantly slower at 2.5 GB. It pays off only once input I/O is a bottleneck
   (10 GB here).
3. **Combined compression with a fast storage codec** (snappy+lz4): never better than lz4 shuffle-only, and
   up to 16 % slower. Use shuffle compression alone instead.
4. **gzip, whenever CPU is limited**: it costs +62 % CPU on A1 for a ratio that zstd beats at one tenth of the
   CPU. Prefer zstd if the goal is space.

Not observed in our setup:

- **Shuffle compression was never harmful.** Keep it on, which is Spark's default (lz4); zstd is better on a
  slow link.
- Our cluster always had spare CPU on the I/O-bound jobs (PC ~2–3 of 16 cores busy during A1). On a fast network
  (10 GbE) with busy CPUs, shuffle compression could become counterproductive. That is an extrapolation from E0,
  not a measurement: one core compresses lz4 at ~800 MB/s, so the break-even is far above our ~40–70 MB/s link
  and would need a much faster network.

---

### Variability and anomalies (for the methodology section)

- Median coefficient of variation of execution time: **2.5 %** over the 100 configurations.
- 5 of 300 runs are outliers (> 25 % and > 10 s above their configuration's median; fig9). Each is caused by a
  single straggler task on the Mac executor:
  - 2 runs hit a 60 s HDFS `ConnectTimeoutException` from the Mac executor to its local DataNode;
  - 3 runs show a ~36–55 s task stall with no error logged.
- The outliers are spread over C0, lz4 and gzip and over 3 sizes, so they are not configuration-dependent. They
  are kept in the dataset; using medians makes the conclusions robust to them.
- Network bytes are measured on the PC's interface only (the Mac VM's counters are unreliable). Per-container CPU
  is unavailable on the Mac (Docker Desktop), so CPU figures use Spark task CPU time from the event log.
