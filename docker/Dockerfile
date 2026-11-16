# One image for every role (namenode, datanode, master, worker, history, client).
# Multi-arch: builds natively on the x86 PC and on the Apple-silicon Mac (arm64).
FROM eclipse-temurin:17-jre

ARG TARGETARCH
ARG HADOOP_VERSION=3.4.3
ARG SPARK_VERSION=4.0.5
# SHA-512 of the release artifacts (from archive.apache.org). Build fails on mismatch.
ARG HADOOP_SHA512_AMD64=e25be7e57b4d3c5bfe83895844321a21d6cf7331266d524d8983c27cf484e576c3d79b3b60d590f8cddecf16229d0e232de2491b1b61b362ee7d67072c7290e1
ARG HADOOP_SHA512_ARM64=2f5c1719b801eedecc5bbed0d4c6d61d7c4850dbb5944fb79da24474f694cb5d0f00b07d1b54cade83f6ca492e1697225f47737cc1a4478ca04102062cf314d5
ARG SPARK_SHA512=c36f0e8445513d3d746d20aa051787f417477a2550b002bb4e4c3797e6152a6a04cce7121344cd11511e18642c852ca5893e872564a207ee9d8d3befe6309b20

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps iperf3 netcat-openbsd iproute2 \
 && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    if [ "$TARGETARCH" = "arm64" ]; then \
        HADOOP_TGZ="hadoop-${HADOOP_VERSION}-aarch64.tar.gz"; HADOOP_SHA="$HADOOP_SHA512_ARM64"; \
    else \
        HADOOP_TGZ="hadoop-${HADOOP_VERSION}.tar.gz"; HADOOP_SHA="$HADOOP_SHA512_AMD64"; \
    fi; \
    # dlcdn is fast but only hosts current releases; archive.apache.org is the permanent (slow) fallback.
    curl -fsSL -o /tmp/hadoop.tgz "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TGZ}" \
      || curl -fsSL -o /tmp/hadoop.tgz "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TGZ}"; \
    echo "${HADOOP_SHA}  /tmp/hadoop.tgz" | sha512sum -c -; \
    tar -xzf /tmp/hadoop.tgz -C /opt; mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop; \
    rm -rf /opt/hadoop/share/doc /tmp/hadoop.tgz; \
    curl -fsSL -o /tmp/spark.tgz "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      || curl -fsSL -o /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"; \
    echo "${SPARK_SHA512}  /tmp/spark.tgz" | sha512sum -c -; \
    tar -xzf /tmp/spark.tgz -C /opt; mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark; \
    rm /tmp/spark.tgz

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/conf \
    SPARK_HOME=/opt/spark \
    SPARK_CONF_DIR=/opt/conf \
    PATH=/opt/hadoop/bin:/opt/spark/bin:$PATH \
    HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root

# Identical config on every node; node-specific values come from env vars (see conf/*.xml, entrypoint.sh).
COPY conf/ /opt/conf/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["client"]
