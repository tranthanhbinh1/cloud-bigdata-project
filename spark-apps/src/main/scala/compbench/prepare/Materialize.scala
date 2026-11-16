package compbench.prepare

import java.net.URI
import java.nio.charset.StandardCharsets

import compbench.common.JobArgs
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Phase 3b — materialise the experiment datasets: <sizes> x <codecs> Parquet copies on HDFS.
  *
  * "N GB" means N * 10^9 bytes of UNCOMPRESSED Parquet (the reference size). Each dataset is a
  * deterministic hash-sample of the canonical dataset, so the sets are NESTED (the 5 GB set contains
  * the 1 GB set) and every size is a uniform sample of all years.
  *
  * args: --input  hdfs:///data/taxi/canonical
  *       --output hdfs:///data/taxi                 -> <output>/<size>gb/<codec>/  (+ MANIFEST.json)
  *       --sizes  1,5,10,20          (default)
  *       --codecs uncompressed,snappy,lz4_raw,gzip,zstd   (default)
  *       --overwrite true            (default: skip datasets that already have _SUCCESS)
  */
object Materialize {
  private val DefaultCodecs = Seq("uncompressed", "snappy", "lz4_raw", "gzip", "zstd")
  private val BlockBytes    = 128L * 1024 * 1024

  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"prep-materialize-${args.runId}").getOrCreate()
    val hconf = spark.sparkContext.hadoopConfiguration
    val fs    = FileSystem.get(new URI(args.output), hconf)

    val sizes     = args.get("sizes").getOrElse("1,5,10,20").split(",").map(_.trim.toInt).toSeq
    val codecs    = args.get("codecs").map(_.split(",").map(_.trim).toSeq).getOrElse(DefaultCodecs)
    val overwrite = args.get("overwrite").contains("true")

    val canonicalBytes = fs.getContentSummary(new Path(args.input)).getLength
    val canonical      = spark.read.parquet(args.input)
    val totalRows      = canonical.count()
    println(f"[materialize] canonical: $totalRows%,d rows, ${canonicalBytes / 1e9}%.2f GB")

    // Deterministic uniform key in [0,1): rows below the threshold p form the nested sample.
    val u = pmod(xxhash64(col("pickup_ts"), col("dropoff_ts"), col("pu_location_id"), col("do_location_id"),
                          col("total_amount"), col("trip_distance"), col("vendor_id")), lit(1L << 30)) / lit((1L << 30).toDouble)

    for (sizeGb <- sizes) {
      val targetBytes = sizeGb * 1000L * 1000L * 1000L
      val p           = math.min(1.0, targetBytes.toDouble / canonicalBytes)
      if (p < targetBytes.toDouble / canonicalBytes + 1e-12 && targetBytes > canonicalBytes)
        println(s"[materialize] WARNING: canonical data (${canonicalBytes / 1e9} GB) is smaller than ${sizeGb} GB; using all of it")
      val numFiles = math.max(8, math.round(targetBytes.toDouble / BlockBytes).toInt)
      val sample   = canonical.withColumn("_u", u).where(col("_u") < p).repartition(numFiles, col("_u")).drop("_u")

      val sizes = scala.collection.mutable.LinkedHashMap[String, Long]()
      for (codec <- codecs) {
        val path = new Path(s"${args.output}/${sizeGb}gb/$codec")
        if (!overwrite && fs.exists(new Path(path, "_SUCCESS"))) {
          println(s"[materialize] ${sizeGb}gb/$codec exists, skipping")
        } else {
          println(s"[materialize] writing ${sizeGb}gb/$codec  (p=${"%.4f".format(p)}, $numFiles files)")
          sample.write.mode("overwrite").option("compression", codec).parquet(path.toString)
        }
        sizes(codec) = fs.getContentSummary(path).getLength
      }

      val rows = spark.read.parquet(s"${args.output}/${sizeGb}gb/${codecs.head}").count()
      val json =
        s"""{"size_gb": $sizeGb, "rows": $rows, "sample_fraction": $p, "num_files": $numFiles,
           | "bytes": {${sizes.map { case (c, b) => s""""$c": $b""" }.mkString(", ")}}}""".stripMargin
      val out = fs.create(new Path(s"${args.output}/${sizeGb}gb/MANIFEST.json"), true)
      out.write(json.getBytes(StandardCharsets.UTF_8)); out.close()
      println(s"[materialize] ${sizeGb}gb manifest: $json")
    }
    spark.stop()
  }
}
