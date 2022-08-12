package com.twitter.finagle.balancersim

import com.twitter.finagle.util.Drv
import com.twitter.finagle.util.Rng
import com.twitter.util.Duration
import scala.io.Source

private object LatencyProfile {
  val rng: Rng = Rng("seed".hashCode)

  /**
   * Creates a latency profile from a file where each line
   * represents recorded latencies.
   */
  def fromFile(path: java.net.URL): () => Duration = {
    val latencies = Source.fromURL(path).getLines.toIndexedSeq.map { line: String =>
      Duration.fromNanoseconds((line.toDouble * 1000000).toLong)
    }

    () => latencies(rng.nextInt(latencies.size))
  }

  /**
   * Returns a function which generates a duration between `low`
   * and `high` when applied.
   */
  def between(low: Duration, high: Duration): () => Duration = {
    require(low <= high)
    () => low + ((high - low) * math.random)
  }

  /**
   * Returns a function which represents the given latency probability distribution.
   */
  def apply(
    min: Duration,
    p50: Duration,
    p90: Duration,
    p95: Duration,
    p99: Duration,
    p999: Duration,
    p9999: Duration
  ): () => Duration = {
    val dist = Seq(
      0.5 -> between(min, p50),
      0.4 -> between(p50, p90),
      0.05 -> between(p90, p95),
      0.04 -> between(p95, p99),
      0.009 -> between(p99, p999),
      0.0009 -> between(p999, p9999)
    )
    val (d, l) = dist.unzip
    apply(d, l.toIndexedSeq)
  }

  /**
   * Creates a function that applies the probability distribution in
   * `dist` over the latency functions in `latencies`.
   */
  def apply(dist: Seq[Double], latencies: IndexedSeq[() => Duration]): () => Duration = {
    val drv = Drv(dist)
    () => latencies(drv(rng))()
  }
}

/**
 * Creates a profile to determine the latency for the next
 * incoming request.
 */
private class LatencyProfile(stopWatch: () => Duration) {

  /** Increase latency returned from `next` by `factor`. */
  def slowBy(factor: Long)(next: () => Duration) = () => { next() * factor }

  /**
   * Increases the latency returned from `next` by `factor` while `stopWatch` is
   * within `start` and `end`.
   */
  def slowWithin(start: Duration, end: Duration, factor: Long)(next: () => Duration) = () => {
    val time = stopWatch()
    if (time >= start && time <= end) next() * factor else next()
  }

  /**
   * Progressively improve latencies returned from `next` while `stopWatch` is still
   * within the window terminated at `end`.
   */
  def warmup(end: Duration, maxFactor: Double = 5.0)(next: () => Duration) = () => {
    val time = stopWatch()
    val factor = if (time < end) (1.0 / time.inNanoseconds) * (end.inNanoseconds) else 1.0
    Duration.fromNanoseconds((next().inNanoseconds * factor.min(maxFactor)).toLong)
  }
}
