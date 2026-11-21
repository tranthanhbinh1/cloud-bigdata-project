package compbench.apps

import compbench.common.{JobArgs, TaxiSchema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** App 2 — CPU-BOUND.
  *
  * One K-means assignment pass with a large k: every row is compared against k fixed centroids
  * (k * 8 distance terms per row), then per-centroid sums and counts are reduced. The compute per row
  * dwarfs the ~35 bytes read, and only k*(8+1) numbers ever cross the network.
  * Fixed seed and no caching: every run does identical work and cost scales linearly with data size.
  *
  * args: --k 512 (default; raise it if the gate shows the app is not CPU-bound enough)
  *
  * Expected profile: CPU > ~80 % on both workers, tiny shuffle, I/O only for the initial read.
  * No compression option is set here: codecs are injected with `--conf` at submit time.
  */
object CpuBoundKMeans {
  // rough natural scale of each feature (same order as TaxiSchema.numericFeatures) so that no feature dominates
  private val scale: Array[Double] = Array(6.0, 20.0, 60.0, 10.0, 10.0, 80.0, 3600.0, 24.0)

  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"A2-cpu-bound-${args.runId}").getOrCreate()
    import spark.implicits._

    val k = args.getInt("k", 512)
    val d = TaxiSchema.numericFeatures.length
    require(scale.length == d, "scale must match TaxiSchema.numericFeatures")

    val rnd       = new scala.util.Random(42)
    val centroids = Array.fill(k)(Array.fill(d)(rnd.nextDouble()))
    val bc        = spark.sparkContext.broadcast(centroids)
    val sc        = scale

    val rows = spark.read.parquet(args.input)
      .select(TaxiSchema.numericFeatures.map(c => col(c).cast("double")): _*)
      .na.drop()

    val (sums, counts) = rows.rdd.mapPartitions { it =>
      val c      = bc.value
      val sum    = Array.ofDim[Double](k, d)
      val count  = new Array[Long](k)
      val x      = new Array[Double](d)
      while (it.hasNext) {
        val r = it.next()
        var j = 0
        while (j < d) { x(j) = r.getDouble(j) / sc(j); j += 1 }
        var best = 0; var bestDist = Double.MaxValue; var i = 0
        while (i < k) {
          val ci = c(i); var dist = 0.0; var m = 0
          while (m < d) { val diff = x(m) - ci(m); dist += diff * diff; m += 1 }
          if (dist < bestDist) { bestDist = dist; best = i }
          i += 1
        }
        count(best) += 1
        j = 0
        while (j < d) { sum(best)(j) += x(j); j += 1 }
      }
      Iterator.single((sum, count))
    }.reduce { case ((s1, c1), (s2, c2)) =>
      var i = 0
      while (i < k) { c1(i) += c2(i); var j = 0; while (j < d) { s1(i)(j) += s2(i)(j); j += 1 }; i += 1 }
      (s1, c1)
    }

    val result = (0 until k).map { i =>
      (i, counts(i), sums(i).map(_ / math.max(1L, counts(i))).toSeq)
    }.toDF("cluster", "count", "centroid")
    result.write.mode("overwrite").parquet(args.output)

    spark.stop()
  }
}
