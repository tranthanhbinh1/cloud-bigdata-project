#!/usr/bin/env bash
# Phase-1 exit check. Run on the PC with the full cluster (PC + Mac) up.
#   1. both Spark workers registered with 4 cores / 6 GB
#   2. SparkPi runs on both executors
#   3. a ~1 GB HDFS write lands blocks on BOTH DataNodes, and reads back intact
#
# Env: EXPECT_WORKERS (default 2)  — set to 1 for the single-machine local smoke test.
set -euo pipefail
EXPECT_WORKERS="${EXPECT_WORKERS:-2}"
CLIENT="${CLIENT:-cb-client}"
sx() { docker exec "$CLIENT" "$@"; }
sxi() { docker exec -i "$CLIENT" "$@"; }   # keeps stdin open (for piping into spark-shell)

echo "== 1. Spark workers"
json=$(sx curl -sf http://cb-pc:8080/json/)
alive=$(echo "$json" | python3 -c 'import sys,json; d=json.load(sys.stdin); w=[x for x in d["workers"] if x["state"]=="ALIVE"]; print(len(w)); [print("  ",x["host"],x["cores"],"cores",x["memory"],"MB") for x in w]' 2>/dev/null || true)
echo "$alive"
[ "$(echo "$alive" | head -1)" = "$EXPECT_WORKERS" ] || { echo "FAIL: expected $EXPECT_WORKERS alive workers"; exit 1; }

echo "== 2. SparkPi"
# (bash -c so the jar glob is expanded inside the container, not passed literally)
sx bash -c 'spark-submit --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.13-*.jar 200 2>&1' \
  | grep -E "Pi is roughly" || { echo "FAIL: SparkPi"; exit 1; }

echo "== 3. HDFS blocks written by the executors land on every DataNode"
sx hdfs dfsadmin -safemode wait 2>&1 | grep -v log4j || true   # NameNode is read-only for ~30 s after a restart
sx hdfs dfs -rm -r -f -skipTrash /healthcheck >/dev/null 2>&1 || true
# Executors write their own partitions; with replication 1 the NameNode places each block on the
# writer's local DataNode, so both DataNodes must end up holding data.
write_out=$(echo 'spark.range(0L, 80000000L, 1, 16).selectExpr("id", "rand() as r", "cast(id as string) as s").write.mode("overwrite").parquet("/healthcheck/data"); println("WRITE_DONE")' \
  | sxi spark-shell 2>&1 || true)
echo "$write_out" | grep -q WRITE_DONE || { echo "FAIL: Spark HDFS write"; echo "$write_out" | tail -15; exit 1; }
sx hdfs dfs -du -s -h /healthcheck/data
echo "  blocks per DataNode:"
per_dn=$(sx hdfs fsck /healthcheck/data -files -blocks -locations 2>/dev/null | grep -oE 'DatanodeInfoWithStorage\[[^:]*' | sed 's/.*\[//' | sort | uniq -c)
echo "$per_dn" | sed 's/^/    /'
[ "$(echo "$per_dn" | grep -c .)" -ge "$EXPECT_WORKERS" ] || { echo "FAIL: blocks not spread over $EXPECT_WORKERS DataNodes"; exit 1; }
rows=$(echo 'println("ROWS=" + spark.read.parquet("/healthcheck/data").count())' | sxi spark-shell 2>&1 | grep -oE 'ROWS=[0-9]+' || true)
echo "  read back: $rows"
[ "$rows" = "ROWS=80000000" ] || { echo "FAIL: row count"; exit 1; }
sx hdfs dfs -rm -r -f -skipTrash /healthcheck >/dev/null
echo "ALL CHECKS PASSED"
