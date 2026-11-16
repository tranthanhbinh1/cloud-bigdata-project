package compbench.apps

import compbench.common.JobArgs
import org.apache.spark.sql.SparkSession

/** App 3 — BALANCED CPU / I/O.
  *
  * Goal: significant computation AND significant data movement, with neither resource dominating.
  *
  * Suggested logic:
  *   1. read the dataset                                                      (args.input)
  *   2. route statistics: groupBy (pu_location_id, do_location_id, pickup_hour) and compute
  *        count, avg/stddev of trip_duration_s, avg fare, and approximate percentiles
  *        (e.g. percentile_approx(trip_duration_s, array(0.5, 0.9, 0.99)))
  *   3. join those statistics BACK to the individual trips (sort-merge join — the config already sets
  *      spark.sql.autoBroadcastJoinThreshold=-1, so no broadcast) and compute, for every trip,
  *      its deviation from its route average (duration - avg_duration, fare / avg_fare, ...)
  *   4. write the enriched trips as Parquet                                   (args.output)
  *
  * Expected profile (Phase-4 characterization gate, 5 GB / config C0):
  *   - substantial CPU (aggregation + sort + join) AND substantial shuffle volume
  *   - neither CPU nor disk/network pinned at saturation
  *
  * Rules: don't set any compression option here. Codecs are injected with --conf at submit time.
  */
object BalancedRouteStats {
  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"A3-balanced-${args.runId}").getOrCreate()

    val trips = spark.read.parquet(args.input)

    // TODO(you): route stats -> join back -> per-trip deviation -> write to args.output
    //   val stats    = trips.groupBy(...).agg(...)
    //   val enriched = trips.join(stats, Seq(...)).withColumn(...)
    //   enriched.write.mode("overwrite").parquet(args.output)
    throw new NotImplementedError("A3 BalancedRouteStats: write the transformation (see the spec above)")

    spark.stop()
  }
}
