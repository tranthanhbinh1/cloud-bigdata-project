package compbench.codecbench

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}
import java.util.zip.{GZIPInputStream, GZIPOutputStream, Deflater}

import com.github.luben.zstd.{ZstdInputStream, ZstdOutputStream}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream}
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

/** E0 — intrinsic codec characteristics: ratio, (de)compression throughput, CPU cost.
  *
  * Single-threaded, in-memory (disk and network are out of the picture), using the same libraries
  * Spark / Parquet use: snappy-java, lz4-java, zstd-jni, java.util.zip.
  *
  * Usage: java -jar codec-bench.jar <sample-file> [--max-mb 512] [--reps 5] [--label pc|mac]
  * Output: CSV on stdout, one row per (codec, rep).
  */
object CodecBench {

  final case class Codec(name: String, level: String,
                         wrap: OutputStream => OutputStream, unwrap: InputStream => InputStream)

  val codecs: Seq[Codec] = Seq(
    Codec("snappy", "-", new SnappyOutputStream(_), new SnappyInputStream(_)),
    Codec("lz4",    "-", new LZ4BlockOutputStream(_, 32 * 1024), new LZ4BlockInputStream(_)),
    Codec("zstd",   "1", new ZstdOutputStream(_, 1), new ZstdInputStream(_)),
    Codec("zstd",   "3", new ZstdOutputStream(_, 3), new ZstdInputStream(_)),   // Spark's default level
    Codec("zstd",   "9", new ZstdOutputStream(_, 9), new ZstdInputStream(_)),
    Codec("gzip",   "1", gz(Deflater.BEST_SPEED, _), new GZIPInputStream(_)),
    Codec("gzip",   "6", gz(6, _), new GZIPInputStream(_)),                    // gzip default
    Codec("gzip",   "9", gz(Deflater.BEST_COMPRESSION, _), new GZIPInputStream(_))
  )

  private def gz(level: Int, out: OutputStream): OutputStream =
    new GZIPOutputStream(out, 64 * 1024) { `def`.setLevel(level) }

  private val threads = ManagementFactory.getThreadMXBean

  /** (wall nanos, thread-CPU nanos) of `body`. Native work (zstd-jni) runs on the calling thread, so it is counted. */
  private def timed[A](body: => A): (A, Long, Long) = {
    val cpu0 = threads.getCurrentThreadCpuTime
    val t0   = System.nanoTime()
    val r    = body
    (r, System.nanoTime() - t0, threads.getCurrentThreadCpuTime - cpu0)
  }

  private def compress(c: Codec, data: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream(data.length / 2)
    val out = c.wrap(bos)
    out.write(data); out.close()
    bos.toByteArray
  }

  private def decompress(c: Codec, comp: Array[Byte], expected: Int): Array[Byte] = {
    val in  = c.unwrap(new ByteArrayInputStream(comp))
    val buf = new Array[Byte](expected)
    var off = 0
    while (off < expected) {
      val n = in.read(buf, off, expected - off)
      if (n < 0) sys.error(s"${c.name}: unexpected EOF at $off/$expected")
      off += n
    }
    in.close()
    buf
  }

  def main(argv: Array[String]): Unit = {
    require(argv.nonEmpty, "usage: CodecBench <sample-file> [--max-mb 512] [--reps 5] [--label host]")
    val opts   = argv.drop(1).sliding(2, 2).collect { case Array(k, v) => k -> v }.toMap
    val maxMb  = opts.getOrElse("--max-mb", "512").toInt
    val reps   = opts.getOrElse("--reps", "5").toInt
    val label  = opts.getOrElse("--label", java.net.InetAddress.getLocalHost.getHostName)
    val arch   = System.getProperty("os.arch")

    val all  = Files.readAllBytes(Paths.get(argv(0)))
    val data = if (all.length > maxMb * 1024 * 1024) java.util.Arrays.copyOf(all, maxMb * 1024 * 1024) else all
    val mb   = data.length / 1048576.0
    System.err.println(f"[codec-bench] $label ($arch): sample ${argv(0)} -> ${mb}%.1f MB, $reps reps")

    println("machine,arch,codec,level,rep,input_mb,compressed_mb,ratio,comp_mb_s,decomp_mb_s,comp_cpu_s_per_gb,decomp_cpu_s_per_gb")
    for (c <- codecs) {
      // warm-up (JIT + native lib load), also verifies the round trip once
      val warm = compress(c, data)
      require(java.util.Arrays.equals(decompress(c, warm, data.length), data), s"round-trip failed for ${c.name}/${c.level}")

      for (rep <- 1 to reps) {
        val (comp, cNs, cCpu) = timed(compress(c, data))
        val (_,    dNs, dCpu) = timed(decompress(c, comp, data.length))
        val compMb = comp.length / 1048576.0
        println(Seq(label, arch, c.name, c.level, rep, f"$mb%.2f", f"$compMb%.2f", f"${mb / compMb}%.3f",
                    f"${mb / (cNs / 1e9)}%.1f", f"${mb / (dNs / 1e9)}%.1f",
                    f"${cCpu / 1e9 / (mb / 1024)}%.3f", f"${dCpu / 1e9 / (mb / 1024)}%.3f").mkString(","))
      }
    }
  }
}
