# Compression in Spark over HDFS — 2-node Dockerized study

USTH InfraCloud project (`ProjectUSTH.pdf`): how storage-level (HDFS/Parquet) and communication-level
(shuffle) compression affect performance and CPU cost, for an I/O-bound, a CPU-bound and a balanced
Spark/Scala workload.

**Cluster:** PC (Linux x86) = Tailscale sidecar + NameNode + DataNode + Spark master + worker + driver + monitoring;
MacBook (M1 Pro, Docker Desktop) = Tailscale sidecar + DataNode + worker. One multi-arch image (`compbench`),
every container shares its machine's Tailscale sidecar network namespace. Full design: see the plan in
`~/.claude/plans/` or `docker/`, `deploy/`.

## Layout
| Path | What |
|---|---|
| `docker/` | image (Hadoop 3.4.3 + Spark 4.0.5), role entrypoint, HDFS/Spark configs (the pinned baseline) |
| `deploy/` | `compose.pc.yml`, `compose.mac.yml`, `compose.pc.local.yml` (no-Tailscale smoke test), monitoring (Prometheus/Grafana), scripts |
| `spark-apps/` | sbt project: `prepare/` (Ingest, Materialize), `apps/` (the 3 minimal workloads), `codec-bench/` (E0) |
| `harness/` | `download_tlc.py` (done); runner / collectors / plots (Phase 5) |
| `results/` | raw measurements + graphs |

## Status
- [x] Phase 1 — image, compose, configs; **single-node smoke test passes** (`deploy/scripts/healthcheck.sh`)
- [x] Phase 2 — Prometheus + Grafana + node-exporter + cAdvisor defined (not yet run: needs Tailscale)
- [x] Phase 3 — `download_tlc.py` (102 months, 8 GB in `data/raw/`), `Ingest`, `Materialize` tested on 3 months;
      codec-bench compiles
- [ ] Phase 0 — Tailscale spike on the Mac (needs your auth key — see below)
- [x] Phase 4a — the 3 minimal apps in `spark-apps/src/main/scala/compbench/apps/` run on 1 month of data
- [ ] Phase 4b — characterization gate (confirm each app's CPU/disk/network profile at 5 GB; needs the 2-node cluster)
- [ ] Phase 5+ — harness, campaign, report

## Quick start

### One-time: Tailscale
1. Create a reusable auth key: https://login.tailscale.com/admin/settings/keys
2. `cp deploy/.env.example deploy/.env` on **both** machines, fill in `TS_AUTHKEY`.
3. Start only the sidecar on each machine, read its tailnet IP, then fill `PC_TS_IP` / `MAC_TS_IP` in both `.env` files:
   ```bash
   docker compose -f deploy/compose.pc.yml  --env-file deploy/.env up -d tailscale   # PC
   docker compose -f deploy/compose.mac.yml --env-file deploy/.env up -d tailscale   # Mac
   docker exec cb-ts-pc  tailscale ip -4      # / cb-ts-mac on the Mac
   ```
   (The sidecar nodes `cb-pc` / `cb-mac` are separate tailnet nodes from the hosts themselves.)

### Mac (Docker Desktop)
Settings → Resources: **8 CPUs, 10–11 GB memory, disk image ≥ 150 GB**. Keep the Mac plugged in, Low Power
Mode off, and run `caffeinate -dimsu` during experiments. Build the image locally:
`docker build -t compbench:latest docker`.

### Bring the cluster up
```bash
docker build -t compbench:latest docker                                   # on each machine
docker compose -f deploy/compose.pc.yml  --env-file deploy/.env up -d     # PC
docker compose -f deploy/compose.mac.yml --env-file deploy/.env up -d     # Mac
deploy/scripts/healthcheck.sh                                             # PC: expects 2 workers, blocks on both DataNodes
```
Single machine, no Tailscale: `docker compose -f deploy/compose.pc.yml -f deploy/compose.pc.local.yml up -d`
then `EXPECT_WORKERS=1 deploy/scripts/healthcheck.sh`.

UIs (over the tailnet): Spark master `cb-pc:8080`, history `cb-pc:18080`, HDFS `cb-pc:9870`, Grafana `cb-pc:3000`.

### Build + run jobs
```bash
cd spark-apps && sbt assembly           # -> target/scala-2.13/compbench.jar  (mounted at /jars/scala-2.13/)
docker exec cb-client spark-submit --class compbench.prepare.Ingest /jars/scala-2.13/compbench.jar \
    --input hdfs://cb-pc:9000/data/raw --output hdfs://cb-pc:9000/data/taxi/canonical
```
Compression is never set in code — always `--conf` (e.g. `--conf spark.io.compression.codec=zstd`).
