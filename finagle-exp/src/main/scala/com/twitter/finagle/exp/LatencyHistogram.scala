package com.twitter.finagle.exp

import com.twitter.util.{Duration, Stopwatch}

/**
 * A concurrent histogram implementation
 * using jsr166e striped adders underneath.
 *
 * This histogram has no dynamic range - it
 * must be configured a priori; but this is
 * appropriate for its application to response
 * latency histograms.
 */
private[finagle] class LatencyHistogram(
    range: Duration, history: Duration, 
    stopwatch: Stopwatch = Stopwatch) {
  require(range > Duration.Zero)

  private[this] val N = range.inMilliseconds.toInt
  private[this] val n = new WindowedAdder(history, 5, stopwatch)
  private[this] val tab = Array.fill(N) { new WindowedAdder(history, 5, stopwatch) }
  
  /**
   * Compute the quantile `which` from the underlying
   * dataset using the normal algorithm without
   * interpolation.
   *
   * @param which the quantile to compute, in [0, 100)
   */
  def quantile(which: Int) = {
    require(which < 100 && which >= 0)
    // The number of samples before
    // the request quantile.
    val t = n.sum()*which/100 + 1
    var i = 0
    var s = 0L
    do {
      s += tab(i).sum()
      i += 1
    } while (i < N && s < t)

    Duration.fromMilliseconds(i-1)    // todo: interpolate?
  }

  def add(d: Duration) {
    require(d >= Duration.Zero)

    val ms = (d min range).inMilliseconds
    tab(ms.toInt).incr()
    n.incr()
  }
}
