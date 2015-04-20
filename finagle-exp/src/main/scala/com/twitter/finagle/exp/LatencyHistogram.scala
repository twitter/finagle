package com.twitter.finagle.exp

import com.twitter.finagle.util.WindowedAdder
import com.twitter.util.Duration

/**
 * A concurrent histogram implementation
 * using jsr166e striped adders underneath.
 *
 * This histogram has no dynamic range - it
 * must be configured a priori; but this is
 * appropriate for its application to response
 * latency histograms.
 *
 * None of the methods on LatencyHistogram entails an allocation,
 * unless invoking `now` does.
 *
 * range, history, and now are expected to have the same units.
 *
 * @param range the maximum duration to measure
 * @param history how long to hold onto data for
 * @param now the current time. for testing.
 */
private[finagle] class LatencyHistogram(
    range: Long,
    history: Long,
    now: () => Long) {

  require(range.toInt > 0)

  private[this] val N = range.toInt + 1
  private[this] val n = new WindowedAdder(history, 5, now)
  private[this] val tab = Array.fill(N) { new WindowedAdder(history, 5, now) }

  /**
   * Compute the quantile `which` from the underlying
   * dataset using the normal algorithm without
   * interpolation.
   *
   * @param which the quantile to compute, in [0, 100)
   */
  def quantile(which: Int): Long = {
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

    i-1 // todo: interpolate?
  }

  /**
   * Adds `d` to the histogram.
   *
   * @param d duration, which should have the same units
   * as the constructor arguments
   */
  def add(d: Long): Unit = {
    require(d >= 0)

    val ms: Long = math.min(d, range)
    tab(ms.toInt).incr()
    n.incr()
  }
}
