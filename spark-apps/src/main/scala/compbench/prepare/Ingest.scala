package compbench.prepare

import java.net.URI

import compbench.common.JobArgs
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Phase 3a — turn the raw TLC monthly Parquet files into ONE canonical dataset.
  *
  * Every month is normalised on its own (column names differ in case between years and numeric types
  * drift: e.g. passenger_count is int64 in 2016 and double in 2022+), then the months are unioned.
  *
  * args: --input  hdfs:///data/raw           (the monthly yellow_tripdata_*.parquet files)
  *       --output hdfs:///data/taxi/canonical (written UNCOMPRESSED: it is the reference for "dataset size")
  */
object Ingest {
  // canonical name -> raw column name (matched case-insensitively) and target type
  private val mapping = Seq(
    ("vendor_id",       "VendorID",              "int"),
    ("pickup_ts",       "tpep_pickup_datetime",  "timestamp"),
    ("dropoff_ts",      "tpep_dropoff_datetime", "timestamp"),
    ("passenger_count", "passenger_count",       "int"),
    ("trip_distance",   "trip_distance",         "double"),
    ("rate_code",       "RatecodeID",            "int"),
    ("pu_location_id",  "PULocationID",          "int"),
    ("do_location_id",  "DOLocationID",          "int"),
    ("payment_type",    "payment_type",          "int"),
    ("fare_amount",     "fare_amount",           "double"),
    ("extra",           "extra",                 "double"),
    ("mta_tax",         "mta_tax",               "double"),
    ("tip_amount",      "tip_amount",            "double"),
    ("tolls_amount",    "tolls_amount",          "double"),
    ("total_amount",    "total_amount",          "double")
  )

  private def normalize(raw: DataFrame): DataFrame = {
    val projected = mapping.map { case (canon, source, tpe) =>
      raw.columns.find(_.equalsIgnoreCase(source)) match {
        case Some(c) => col(s"`$c`").cast(tpe).as(canon)
        case None    => lit(null).cast(tpe).as(canon)
      }
    }
    raw.select(projected: _*)
  }

  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"prep-ingest-${args.runId}").getOrCreate()

    val fs    = FileSystem.get(new URI(args.input), spark.sparkContext.hadoopConfiguration)
    val files = fs.listStatus(new Path(args.input)).map(_.getPath.toString).filter(_.endsWith(".parquet")).sorted
    require(files.nonEmpty, s"no .parquet files under ${args.input}")
    println(s"[ingest] ${files.length} monthly files")

    val all = files.map(f => normalize(spark.read.parquet(f))).reduce(_ unionByName _)

    val canonical = all
      .withColumn("trip_duration_s", col("dropoff_ts").cast("long") - col("pickup_ts").cast("long"))
      .where(
        col("pickup_ts") >= lit("2016-07-01") && col("pickup_ts") < lit("2025-01-01") &&
        col("trip_duration_s").between(1L, 86400L) &&
        col("passenger_count").between(0, 9) &&
        col("trip_distance").between(0.0, 500.0) &&
        col("total_amount").between(0.0, 2000.0) &&
        col("pu_location_id").isNotNull && col("do_location_id").isNotNull)
      .withColumn("pickup_hour", hour(col("pickup_ts")))
      .withColumn("pickup_date", to_date(col("pickup_ts")))

    canonical.write.mode("overwrite").option("compression", "uncompressed").parquet(args.output)

    val outFs   = FileSystem.get(new URI(args.output), spark.sparkContext.hadoopConfiguration)
    val bytes   = outFs.getContentSummary(new Path(args.output)).getLength
    val rows    = spark.read.parquet(args.output).count()
    println(f"[ingest] canonical: $rows%,d rows, ${bytes / 1e9}%.2f GB uncompressed Parquet, ${bytes.toDouble / rows}%.1f B/row")
    spark.stop()
  }
}
