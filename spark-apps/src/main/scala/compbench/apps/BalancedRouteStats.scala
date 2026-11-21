package compbench.apps

import compbench.common.JobArgs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** App 3 — BALANCED CPU / I/O.
  *
  * Real computation AND real data movement:
  *   1. per-route statistics: groupBy (pickup zone, dropoff zone, hour) -> count, avg/stddev of duration,
  *      median duration, avg fare  (aggregation + percentile = CPU)
  *   2. join the statistics back onto every trip (sort-merge join: the baseline config disables broadcast
  *      joins) and compute each trip's deviation from its route average  (shuffle of all trips + sort = I/O + CPU)
  *   3. write the per-trip deviations
  *
  * Expected profile: substantial CPU and substantial shuffle, neither pinned at saturation.
  * No compression option is set here: codecs are injected with `--conf` at submit time.
  */
object BalancedRouteStats {
  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"A3-balanced-${args.runId}").getOrCreate()

    val trips = spark.read.parquet(args.input)
    val route = Seq("pu_location_id", "do_location_id", "pickup_hour")

    val stats = trips.groupBy(route.map(col): _*).agg(
      count(lit(1)).as("n"),
      avg("trip_duration_s").as("avg_duration"),
      stddev("trip_duration_s").as("std_duration"),
      percentile_approx(col("trip_duration_s"), lit(0.5), lit(1000)).as("median_duration"),
      avg("fare_amount").as("avg_fare"))

    trips.join(stats, route)
      .select(
        col("pickup_date"), col("pu_location_id"), col("do_location_id"), col("pickup_hour"),
        (col("trip_duration_s") - col("avg_duration")).as("duration_dev"),
        (col("trip_duration_s") - col("median_duration")).as("duration_dev_median"),
        // routes whose average fare is 0 exist; Spark 4 runs in ANSI mode, where x / 0 is an error
        when(col("avg_fare") > 0, col("fare_amount") / col("avg_fare")).as("fare_ratio"))
      .write.mode("overwrite").parquet(args.output)

    spark.stop()
  }
}
