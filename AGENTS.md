# AGENTS.md

Guidance for AI coding agents working in this repository. Humans: start with `README.md` (setup) and
`report/report.md` (what was found).

## What this repo is

The USTH InfraCloud project (`ProjectUSTH.pdf`): an experimental study of storage-level (Parquet on HDFS) and
communication-level (Spark shuffle) compression. The cluster is two machines running Docker:

- **PC:** Linux x86, with 4 Spark workers.
- **MacBook:** M1 Pro, with 1 Spark worker.

The machines talk over Wi-Fi.

**Status: finished.** The campaign is done (300 runs, 0 failures), the report is written (`report/report.pdf`),
and the slides are owned by other team members. Unless asked, treat the results as read-only.

## Ground rules

- **`results/raw/` is the dataset of record.** Do not delete, overwrite or re-run individual runs in it. Re-running
  only the slow outliers would bias the medians: they are explained in the report (§9) and kept on purpose. New
  experiments go into a new directory (for example `results/raw-<name>/`) and must not be mixed with the old ones.
- **`docker/conf/spark-defaults.conf` is the frozen baseline.** Every run used it. Compression is never set in code
  or in that file, only per run with `--conf` from `harness/runner.py`. Changing the baseline (cores, memory,
  partitions, AQE…) invalidates any comparison with existing results.
- **Significance rule:** two configurations differ only when their min..max ranges over the 3 runs do not overlap.
  Medians are reported and plotted with min..max error bars. Do not describe a difference as real if it fails this
  test; call it a trend.
- **Report numbers come from the data.** Recompute them from `results/summary/*.csv`; never estimate them. The
  report has been corrected once already for over-claiming (A1's 10 GB storage-only gain is a trend only).

## Regenerating outputs

```bash
python3 harness/collect.py     # results/raw -> results/summary/{runs,cells,ratios,outliers}.csv
python3 harness/plots.py       # -> results/plots/fig1..fig9 (matplotlib)
report/build.sh                # report/report.md -> report/report.pdf (pandoc + headless Chrome)
```

`report/report.md` is the source of truth. Edit it, then rebuild the PDF; never edit the PDF.

## Running anything on the cluster

1. Check health first: `deploy/scripts/healthcheck.sh` should show 5 workers (4 `cb-pc`, 1 `cb-mac`) and blocks on
   both DataNodes.
2. `harness/runner.py --dry-run` prints the plan and a time estimate. Runs are resumable: a run with a
   `result.json` is skipped.
3. The Mac is driven from the PC through the Docker context `mac` (`ssh://cb-mac-host`, key-based SSH).

## Known traps (symptom → cause → fix)

- **CPU, disk and network fields in `result.json` are empty, or Prometheus targets show "out of bounds".**
  The PC dual-boots Windows; after a reboot its clock was ~7 h fast until NTP corrected it, and Prometheus stored
  future-dated samples. Check the head maxTime at `http://127.0.0.1:9090/api/v1/status/tsdb`. To recover,
  move the `compbench-pc_prometheus-data` volume's contents into an archive folder and restart `cb-prometheus`.
  `runner.py` now refuses to run while samples are stale.
- **A Mac port is "connection refused" although the container is Up.** After Docker Desktop restarts, it
  sometimes does not re-publish host-network ports. `docker --context mac restart <container>`.
- **Parquet `lz4_raw` gives `MalformedInputException` on the Mac executor.** Its pure-Java decoder fails on ARM64.
  The study uses Parquet `lz4` (Hadoop LZ4) instead.
- **Mac services unreachable from the PC.** Docker Desktop host networking only forwards wildcard listeners, so
  inside the Mac containers `cb-mac` must resolve to `0.0.0.0` (`deploy/compose.mac.yml`). "Enable host
  networking" must be on in Docker Desktop.
- **HDFS RPC is on port 8020, not 9000.** MinIO already uses 9000 on the PC.
- **Shuffle files land in the container's overlay filesystem.** In standalone mode the worker's
  `SPARK_LOCAL_DIRS` overrides `spark.local.dir`; it is set in both Compose files.
- **The event log "is not found".** Spark 4 writes rolling directories: `/spark-events/eventlog_v2_<appId>/events_*`.
- **Network bytes:** use only the PC's `enp7s0` counters. The Mac VM's `eth0` misses flows the PC initiates.
- **The Mac becomes sluggish or swaps.** Keep Docker Desktop at 6 CPUs / 10 GB; more memory pushes the 16 GB Mac
  into swap.
- **HDFS is read-only for ~30 s after a restart (safemode).** `hdfs dfsadmin -safemode wait`.

## Where things are

| Path | Contents |
|---|---|
| `docker/`, `deploy/` | Image, configs, Compose files, monitoring, helper scripts (health, network, cache flush) |
| `spark-apps/` | sbt project: `prepare/` (Ingest, Materialize), `apps/` (A1 IoBoundEtl, A2 CpuBoundKMeans, A3 BalancedRouteStats), `codec-bench/` (E0) |
| `harness/` | `runner.py` (campaign), `characterize.py` (workload gate and shared helpers), `collect.py`, `plots.py`, `download_tlc.py` |
| `results/` | `raw/` (one `result.json` per run), `summary/`, `plots/`, `codec-bench/` (E0), `characterization/` (gate history, earlier noise runs), `logs/` |
| `report/` | `report.md` (source), `report.pdf`, `build.sh`, `report.css` |

## Security

- Spark standalone and HDFS run **without authentication**. Never expose them beyond the trusted LAN.
- Never commit `deploy/.env` (it is gitignored) or any SSH keys.
- Do not add personal details of the team to outside requests.
