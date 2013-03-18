package com.twitter.finagle.exp

import com.twitter.jsr166e.LongAdder
import com.twitter.util.{Duration, Stopwatch}
import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

/**
 * A time-windowed version of a LongAdder.
 *
 * @param range The range of time to be kept in the adder.
 *
 * @param slices The number of slices that are maintained; a higher
 * number of slices means finer granularity but also more memory
 * consumption. Must be at least 1.
 */
private class WindowedAdder(
    range: Duration, slices: Int, 
    stopwatch: Stopwatch = Stopwatch) {
  require(slices > 1)
  private[this] val window = range/slices
  private[this] val N = slices-1

  private[this] val writer = new LongAdder
  @volatile private[this] var gen = 0
  private[this] val expiredGen = new AtomicInteger(gen)

  // Since we only write into the head bucket, we simply maintain
  // counts in an array; these are written to rarely, but are read
  // often.
  private[this] val buf = new Array[Long](N)
  @volatile private[this] var i = 0
  @volatile private[this] var elapsed = stopwatch.start()

  private[this] def expired() {
    if (!expiredGen.compareAndSet(gen, gen+1))
      return
      
    // At the time of add, we were likely up to date,
    // so we credit it to the current slice.
    buf(i) = writer.sumThenReset()
    i = (i+1)%N

    // If it turns out we've skipped a number of
    // slices, we adjust for that here.
    val nskip = (elapsed().inMilliseconds/
      window.inMilliseconds-1).toInt min N
    if (nskip > 0) {
      val r = nskip min (N-i)
      Arrays.fill(buf, i, i+r, 0L)
      Arrays.fill(buf, 0, nskip - r, 0L)
      i = (i+nskip)%N
    }

    elapsed = stopwatch.start()
    gen += 1
  }

  /** Reset the state of the adder */
  def reset() {
    Arrays.fill(buf, 0, N, 0L)
    writer.reset()
    elapsed = stopwatch.start()
  }
 
  /** Increment the adder by 1 */
  def incr() = add(1)

  /** Increment the adder by `x` */
  def add(x: Int) {
    if (elapsed() >= window)
      expired()
    writer.add(x)
  }

  /** Retrieve the current sum of the adder */
  def sum(): Long = {
    if (elapsed() >= window)
      expired()
    val _ = gen  // Barrier.
    var sum = writer.sum()
    var i = 0
    while (i < N) {
      sum += buf(i)
      i += 1
    }
    sum
  }
}
