package compbench.common

/** Minimal `--key value` argument parser shared by every job (no extra dependency).
  *
  * The harness always passes `--input`, `--output`, `--run-id`. Compression is NEVER set here:
  * it comes from `spark-submit --conf` (spark.sql.parquet.compression.codec, spark.io.compression.codec,
  * spark.shuffle.compress, ...), so the apps stay identical across all configurations.
  */
final case class JobArgs(input: String, output: String, runId: String, extra: Map[String, String]) {
  def get(key: String): Option[String] = extra.get(key)
  def getInt(key: String, default: Int): Int = extra.get(key).map(_.toInt).getOrElse(default)
}

object JobArgs {
  def parse(argv: Array[String]): JobArgs = {
    val kv = argv.sliding(2, 2).collect {
      case Array(k, v) if k.startsWith("--") => k.drop(2) -> v
    }.toMap
    def req(k: String) = kv.getOrElse(k, sys.error(s"missing required argument --$k"))
    JobArgs(req("input"), req("output"), kv.getOrElse("run-id", "adhoc"),
            kv -- Seq("input", "output", "run-id"))
  }
}
