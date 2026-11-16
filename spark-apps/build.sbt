ThisBuild / scalaVersion := "2.13.16"   // Spark 4.0.x is built for Scala 2.13
ThisBuild / organization := "compbench"
ThisBuild / version      := "0.1.0"

val sparkVersion = "4.0.5"

// Spark jobs: prepare (Ingest/Materialize) + the three workload apps.
// Spark itself is "provided": the cluster image already ships it.
// Build the fat jar with `sbt assembly` -> target/scala-2.13/compbench.jar
// (mounted read-only at /jars/scala-2.13/compbench.jar inside the cb-client container).
lazy val root = (project in file("."))
  .settings(
    name := "compbench",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"   % sparkVersion % Provided,
      "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided   // optional: only if you want MLlib helpers
    ),
    assembly / assemblyJarName := "compbench.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    },
    scalacOptions ++= Seq("-deprecation", "-feature", "-Wunused:imports")
  )

// Intrinsic codec benchmark (E0): plain JVM, no Spark. Run with `sbt "codecBench/run <args>"`
// or `sbt codecBench/assembly` and `java -jar`.
lazy val codecBench = (project in file("codec-bench"))
  .settings(
    name := "codec-bench",
    libraryDependencies ++= Seq(
      // the same codec libraries Spark/Parquet use under the hood
      "org.xerial.snappy"  % "snappy-java" % "1.1.10.7",
      "org.lz4"            % "lz4-java"    % "1.8.0",
      "com.github.luben"   % "zstd-jni"    % "1.5.6-9"
    ),
    assembly / assemblyJarName := "codec-bench.jar",
    assembly / mainClass := Some("compbench.codecbench.CodecBench"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _                                   => MergeStrategy.first
    },
    fork := true
  )
