package compbench.apps

import compbench.common.JobArgs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** App 3 — BALANCED CPU / I/O.
  *
  * Real computation AND real data movement, tuned so that neither dominates on this cluster. The PC<->Mac link is
  * ~35 MB/s Wi-Fi, so anything that shuffles whole rows (e.g. joining statistics back onto every trip) is network-bound
  * (measured at 5 GB: 8.6 GB shuffled, executor CPU 18-24 %, link saturated). Instead:
  *      (5 GB gate: by hour -> ~1.6M groups -> 6.5 GB of partial aggregates, link saturated, 243 s;
  *       by (pu,do) only -> ~70K groups -> 0.84 GB shuffle, CPU-leaning, link ~20 % busy;
  *       the key granularity is the dial that sets how much data moves relative to the computation)
  *   1. per-route statistics: groupBy (pickup zone, dropoff zone, 6-hour day part) -> count, mean/stddev/skewness/kurtosis of
  *      duration, fare, distance and tip, and a couple of derived ratios  (many aggregates = CPU; map-side combine keeps the
  *      shuffle to partial aggregates = moderate data movement)
  *   2. global sort by trip count (a second shuffle + range partitioning)
  *   3. write the statistics table
  *
  * Expected profile: substantial CPU and substantial shuffle, neither pinned at saturation.
  * No compression option is set here: codecs are injected with `--conf` at submit time.
  */
object BalancedRouteStats {
  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"A3-balanced-${args.runId}").getOrCreate()

    val route = Seq("pu_location_id", "do_location_id", "day_part")
    val trips = spark.read.parquet(args.input)
      .where(col("trip_distance") > 0 && col("trip_duration_s") > 0)
      .withColumn("day_part", (col("pickup_hour") / 6).cast("int"))     // 4 buckets of 6 h
      .withColumn("speed_mph", col("trip_distance") / (col("trip_duration_s") / 3600.0))
      .withColumn("fare_per_mile", col("fare_amount") / col("trip_distance"))
      .withColumn("tip_pct", when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")))

    def moments(c: String) = Seq(
      avg(c).as(s"${c}_avg"), stddev(c).as(s"${c}_std"), skewness(c).as(s"${c}_skew"), kurtosis(c).as(s"${c}_kurt"))

    val cols = Seq("trip_duration_s", "fare_amount", "trip_distance", "tip_amount", "speed_mph", "fare_per_mile", "tip_pct")
    val aggs = count(lit(1)).as("n") +: cols.flatMap(moments) :+ max("total_amount").as("max_total")

    trips.groupBy(route.map(col): _*).agg(aggs.head, aggs.tail: _*)
      .orderBy(col("n").desc)
      .write.mode("overwrite").parquet(args.output)

    spark.stop()
  }
}
