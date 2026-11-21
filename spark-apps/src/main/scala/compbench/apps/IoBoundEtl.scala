package compbench.apps

import compbench.common.JobArgs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** App 1 — I/O-BOUND.
  *
  * Moves every byte through disk and network with almost no computation:
  * read everything -> full shuffle (hash-repartition by pickup_date) -> write everything back.
  *
  * Expected profile (checked in the characterization gate): low CPU, disk/network near saturation,
  * shuffle bytes ~ input size.
  *
  * No compression option is set here: codecs are injected with `--conf` at submit time.
  */
object IoBoundEtl {
  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"A1-io-bound-${args.runId}").getOrCreate()

    spark.read.parquet(args.input)
      .repartition(col("pickup_date"))          // shuffle: every row crosses the network
      .write.mode("overwrite").parquet(args.output)

    spark.stop()
  }
}
