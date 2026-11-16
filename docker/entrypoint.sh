#!/usr/bin/env bash
# Role selector. Usage: entrypoint.sh <namenode|datanode|master|worker|history|client>
#
# Required env (set in the compose files):
#   NODE_HOST      logical hostname of THIS machine (cb-pc | cb-mac), resolvable via /etc/hosts
#   MASTER_HOST    logical hostname of the machine running NameNode + Spark master (cb-pc)
# Optional: WORKER_CORES, WORKER_MEMORY
set -euo pipefail

: "${NODE_HOST:?NODE_HOST must be set}"
: "${MASTER_HOST:?MASTER_HOST must be set}"
# NODE_HOST / MASTER_HOST are also read directly by core-site.xml / hdfs-site.xml via ${env.*}
# (they come from the compose `environment:`, so they are visible to `docker exec` sessions too).
export SPARK_LOCAL_HOSTNAME="$NODE_HOST"
export SPARK_MASTER_URL="spark://${MASTER_HOST}:7077"

role="${1:-client}"
case "$role" in
  namenode)
    if [ ! -f /hdfs/name/current/VERSION ]; then
      echo "[entrypoint] formatting NameNode"
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode
    ;;
  datanode)
    exec hdfs datanode
    ;;
  master)
    exec spark-class org.apache.spark.deploy.master.Master \
      --host "$NODE_HOST" --port 7077 --webui-port 8080
    ;;
  worker)
    exec spark-class org.apache.spark.deploy.worker.Worker "$SPARK_MASTER_URL" \
      --host "$NODE_HOST" --webui-port 8081 \
      --cores "${WORKER_CORES:-4}" --memory "${WORKER_MEMORY:-6g}" \
      --work-dir /spark-work
    ;;
  history)
    export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events"
    exec spark-class org.apache.spark.deploy.history.HistoryServer
    ;;
  client)
    # Idle container; jobs are submitted with `docker exec <client> spark-submit ...`
    exec sleep infinity
    ;;
  *)
    echo "unknown role: $role" >&2; exit 2
    ;;
esac
