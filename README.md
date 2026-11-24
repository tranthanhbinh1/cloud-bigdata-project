# Compression in Spark over HDFS — 2-node Dockerized study

USTH InfraCloud project (`ProjectUSTH.pdf`): how storage-level (HDFS/Parquet) and communication-level
(shuffle) compression affect performance and CPU cost, for an I/O-bound, a CPU-bound and a balanced
Spark/Scala workload.

**Cluster:** PC (Linux x86) = NameNode + DataNode + Spark master + **4 workers** + driver + monitoring;
MacBook (M1 Pro, Docker Desktop) = DataNode + 1 worker. All 5 workers/executors are identical (4 cores, 5 GB heap), so the
PC is simply 4 'standard nodes' on one machine (16 of its 20 logical CPUs) and the Mac is 1. One multi-arch image (`compbench`). Every container uses
`network_mode: host`, so PC<->Mac traffic is plain LAN traffic (no overlay). On the Mac this needs Docker Desktop's
**host networking** (see below), and `cb-mac` resolves to `0.0.0.0` inside the Mac containers so services bind the
wildcard, which Docker Desktop then exposes on the Mac's LAN address. Measured link (iperf3, 4 streams):
Mac->PC ~265-300 Mbit/s, PC->Mac ~525-550 Mbit/s (Wi-Fi). Full design: see the plan in `~/.claude/plans/`.

> Tailscale was used first and abandoned: single-/multi-stream throughput between the sidecars swung between
> 38 and 285 Mbit/s while raw LAN stayed at ~300 Mbit/s, which would add unexplained noise to every measurement.

## Layout
| Path | What |
|---|---|
| `docker/` | image (Hadoop 3.4.3 + Spark 4.0.5), role entrypoint, HDFS/Spark configs (the pinned baseline) |
| `deploy/` | `compose.pc.yml`, `compose.mac.yml`, monitoring (Prometheus/Grafana), scripts |
| `spark-apps/` | sbt project: `prepare/` (Ingest, Materialize), `apps/` (the 3 minimal workloads), `codec-bench/` (E0) |
| `harness/` | `download_tlc.py`, `characterize.py` (workload gate), `runner.py` (campaign), `collect.py` (summary CSVs), `plots.py` (figures) |
| `results/` | `raw/` (one `result.json` per run), `summary/` (CSVs), `plots/` (figures), `codec-bench/` (E0), `characterization/`, `logs/` |
| `report/` | `report.md` / `report.pdf` (technical report, incl. answers to Q1–Q7); `build.sh` rebuilds the PDF |

## Status
- [x] Phase 1 — image, compose, configs; **single-node smoke test passes** (`deploy/scripts/healthcheck.sh`)
- [x] Phase 2 — Prometheus + Grafana + node-exporter + cAdvisor defined (running)
- [x] Phase 3 — `download_tlc.py` (102 months, 8 GB in `data/raw/`), `Ingest`, `Materialize` tested on 3 months;
      codec-bench compiles
- [x] Phase 0 — two-node cluster up over the raw LAN; `healthcheck.sh` passes with 2 workers
- [x] Phase 4a — the 3 minimal apps in `spark-apps/src/main/scala/compbench/apps/` run on 1 month of data
- [x] Phase 4b — characterization gate passed at 5 GB (A1 I/O-bound, A2 CPU-bound, A3 balanced): `results/characterization/SUMMARY.md`
- [x] Phase 5 — runner, campaign: **300 runs** (4 sizes × 25 app-configs × 3 reps), 0 failures, median CV 2.5 %
- [x] Phase 6 — E0 codec benchmark (PC + Mac), aggregation, figures, technical report (`report/report.pdf`)
- [x] Phase 7 — slides for the oral presentation (`slides/slides.tex` -> `slides/slides.pdf`)

## Quick start

### One-time setup
1. Give the PC and the Mac a DHCP reservation in your router so their LAN IPs do not change.
2. `cp deploy/.env.example deploy/.env` on **both** machines and set `PC_LAN_IP` and `MAC_LAN_IP`.
3. **Mac:** Docker Desktop > Settings > Resources > Network > **Enable host networking**; CPUs 6, memory 10 GB
   (more memory pushes a 16 GB Mac into swap), disk image >= 150 GB. Keep the Mac plugged in, Low Power Mode off, and run `caffeinate -dimsu` during experiments.
4. **PC -> Mac control channel** (runs the Mac side from the PC, flushes the Mac's page cache): enable Remote Login on
   the Mac, create a dedicated key (`ssh-keygen -t ed25519 -f ~/.ssh/compbench_mac`), authorize its public key on the
   Mac, add a `Host cb-mac-host` entry to `~/.ssh/config`, and `docker context create mac --docker host=ssh://cb-mac-host`.
   On the Mac add `export PATH=/usr/local/bin:$PATH` to `~/.zshenv` (non-interactive SSH has no `docker` on its PATH).
5. **Security:** Spark standalone and HDFS run without authentication on your LAN. Use a trusted network, or restrict
   the PC with a firewall to the Mac's address.

### Bring the cluster up
```bash
docker build -t compbench:latest docker                                         # on each machine (or: docker --context mac build ...)
docker compose -f deploy/compose.pc.yml  --env-file deploy/.env up -d           # PC
docker --context mac compose -f deploy/compose.mac.yml --env-file deploy/.env up -d   # Mac (from the PC, or run locally on the Mac)
deploy/scripts/healthcheck.sh                                                   # expects 5 workers, blocks on both DataNodes
deploy/scripts/netcheck.sh                                                      # LAN throughput, one JSON line
```
Single machine: start only `compose.pc.yml` and run `EXPECT_WORKERS=4 EXPECT_DATANODES=1 deploy/scripts/healthcheck.sh`.

UIs: Spark master `cb-pc:8080`, history `cb-pc:18080`, HDFS `cb-pc:9870`, Grafana `cb-pc:3000`
(HDFS RPC is on port 8020 because 9000 is commonly taken).

### Build + run jobs
```bash
cd spark-apps && sbt assembly           # -> target/scala-2.13/compbench.jar  (mounted at /jars/scala-2.13/)
docker exec cb-client spark-submit --class compbench.prepare.Ingest /jars/scala-2.13/compbench.jar \
    --input hdfs://cb-pc:8020/data/raw --output hdfs://cb-pc:8020/data/taxi/canonical
```
Compression is never set in code — always `--conf` (e.g. `--conf spark.io.compression.codec=zstd`).

### Run the study
```bash
python3 harness/runner.py --dry-run                                              # plan + time estimate
systemd-inhibit --what=idle:sleep python3 harness/runner.py --sizes 0.5 2.5 5 10 --reps 3   # ~10 h, resumable
python3 harness/collect.py && python3 harness/plots.py                           # results/summary, results/plots
report/build.sh                                                                  # report/report.pdf
```
