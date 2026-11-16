package compbench.apps

import compbench.common.{JobArgs, TaxiSchema}
import org.apache.spark.sql.SparkSession

/** App 2 — CPU-BOUND.
  *
  * Goal: runtime dominated by computation, not data movement.
  *
  * Suggested logic: a HAND-WRITTEN K-means (don't call MLlib's KMeans — we want to own the
  * per-iteration work and keep the shuffle tiny):
  *   1. read the dataset, keep only `TaxiSchema.numericFeatures`, drop nulls
  *   2. standardise the features (mean / stddev), and cache() the standardised data
  *   3. pick k initial centroids (fixed seed!), with k large (~50-100) so the distance computation is heavy
  *   4. repeat ~10 iterations (take from args.getInt("iterations", 10)):
  *        - broadcast the centroids
  *        - for each row find the nearest centroid (k x 8 distance computations)
  *        - aggregate per-centroid sum + count (map-side combine, e.g. reduceByKey / aggregate)
  *        - compute the new centroids on the driver
  *   5. write the final centroids (tiny) to args.output as Parquet/CSV
  *
  * Expected profile (Phase-4 characterization gate, 5 GB / config C0):
  *   - CPU > ~80 % on both workers during the iterations
  *   - shuffle volume tiny (only per-centroid partial sums)
  *   - disk / network I/O only for the initial read
  *
  * Tip: use a fixed seed everywhere so repeated runs do identical work.
  * Rules: don't set any compression option here. Codecs are injected with --conf at submit time.
  */
object CpuBoundKMeans {
  def main(argv: Array[String]): Unit = {
    val args  = JobArgs.parse(argv)
    val spark = SparkSession.builder().appName(s"A2-cpu-bound-${args.runId}").getOrCreate()
    val k          = args.getInt("k", 64)
    val iterations = args.getInt("iterations", 10)

    val features = spark.read.parquet(args.input).select(TaxiSchema.numericFeatures.head, TaxiSchema.numericFeatures.tail: _*)

    // TODO(you): standardise, cache, run `iterations` rounds of K-means with `k` centroids,
    //            then write the centroids to args.output
    throw new NotImplementedError(s"A2 CpuBoundKMeans: write the K-means loop (k=$k, iterations=$iterations; ${features.columns.length} features)")

    spark.stop()
  }
}
