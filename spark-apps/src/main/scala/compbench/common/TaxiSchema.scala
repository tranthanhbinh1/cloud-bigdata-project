package compbench.common

import org.apache.spark.sql.types._

/** Canonical schema of the prepared NYC Yellow Taxi dataset (written by `Ingest`, read by all apps).
  *
  * Only 2016-07 onward is used: from then on trips carry TLC zone IDs (pu/do_location_id) instead of
  * lat/lon, which App 3 groups on.
  */
object TaxiSchema {
  val schema: StructType = StructType(Seq(
    StructField("vendor_id",        IntegerType),
    StructField("pickup_ts",        TimestampType),
    StructField("dropoff_ts",       TimestampType),
    StructField("passenger_count",  IntegerType),
    StructField("trip_distance",    DoubleType),
    StructField("rate_code",        IntegerType),
    StructField("pu_location_id",   IntegerType),
    StructField("do_location_id",   IntegerType),
    StructField("payment_type",     IntegerType),
    StructField("fare_amount",      DoubleType),
    StructField("extra",            DoubleType),
    StructField("mta_tax",          DoubleType),
    StructField("tip_amount",       DoubleType),
    StructField("tolls_amount",     DoubleType),
    StructField("total_amount",     DoubleType),
    // derived by Ingest
    StructField("trip_duration_s",  LongType),
    StructField("pickup_hour",      IntegerType),
    StructField("pickup_date",      DateType)
  ))

  /** Numeric columns suitable as features for the CPU-bound app. */
  val numericFeatures: Seq[String] = Seq(
    "passenger_count", "trip_distance", "fare_amount", "tip_amount",
    "tolls_amount", "total_amount", "trip_duration_s", "pickup_hour")
}
