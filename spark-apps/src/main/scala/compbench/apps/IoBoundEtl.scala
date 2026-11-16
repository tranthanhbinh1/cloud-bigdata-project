package compbench.apps

import compbench.common.JobArgs
import org.apache.spark.sql.SparkSession

/** App 1 — I/O-BOUND.
  *
  * Goal: runtime dominated by moving bytes (disk read, shuffle over the network, disk write), with
  * almost no computation per byte.
  *
  * Suggested logic (feel free to change, but keep the profile below):
  *   1. read ALL columns of the prepared taxi dataset                    (args.input)
  *   2. drop clearly invalid rows with a cheap filter (e.g. trip_distance > 0, total_amount > 0)
  *   3. repartition by `pickup_date`  -> full shuffle of ~all input bytes
  *   4. write the result back as Parquet                                 (args.output)
  *
  * Expected profile (checked in the Phase-4 characterization gate, 5 GB / config C0):
  *   - low CPU utilisation on both workers
  *   - disk and/or network throughput close to saturation
  *   - shuffle bytes written ~ input size
  *
  * Rules: don't set any compression option here. Codecs are injected with --conf at submit time.
  */
object IoBoundEtl {
  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"A1-io-bound-${args.runId}").getOrCreate()

    val trips = spark.read.parquet(args.input)

    // TODO(you): filter, repartition by pickup_date, and write to args.output
    //   val result = trips.???
    //   result.write.mode("overwrite").parquet(args.output)
    throw new NotImplementedError("A1 IoBoundEtl: write the transformation (see the spec above)")

    spark.stop()
  }
}
