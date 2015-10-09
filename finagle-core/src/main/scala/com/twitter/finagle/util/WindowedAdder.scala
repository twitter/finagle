package com.twitter.finagle.util

import com.twitter.jsr166e.LongAdder
import com.twitter.util.Time
import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

private[finagle] object WindowedAdder {

  /**
   * Create a time-windowed version of a LongAdder.
   *
   * None of the operations on this data structure entails an allocation,
   * unless invoking now does.
   *
   * `range` and `now` are expected to have the same units.
   *
   * @param range The range of time to be kept in the adder.
   *
   * @param slices The number of slices that are maintained; a higher
   * number of slices means finer granularity but also more memory
   * consumption. Must be more than 1.
   *
   * @param now the current time. for testing.
   */
  def apply(range: Long, slices: Int, now: () => Long): WindowedAdder = {
    require(slices > 1)
    new WindowedAdder(range/slices, slices-1, now)
  }

  // we use nanos instead of current time millis because it increases monotonically
  val systemMs: () => Long = () => System.nanoTime() / (1000 * 1000)
  val timeMs: () => Long = () => Time.now.inMilliseconds
}

private[finagle] class WindowedAdder private[WindowedAdder](
    window: Long,
    N: Int,
    now: () => Long)
{
  private[this] val writer = new LongAdder()
  @volatile private[this] var gen = 0
  private[this] val expiredGen = new AtomicInteger(gen)

  // Since we only write into the head bucket, we simply maintain
  // counts in an array; these are written to rarely, but are read
  // often.
  private[this] val buf = new Array[Long](N)
  @volatile private[this] var i = 0
  @volatile private[this] var old = now()

  private[this] def expired(): Unit = {
    if (!expiredGen.compareAndSet(gen, gen+1))
      return

    // At the time of add, we were likely up to date,
    // so we credit it to the current slice.
    buf(i) = writer.sumThenReset()
    i = (i+1)%N

    // If it turns out we've skipped a number of
    // slices, we adjust for that here.
    val nskip = math.min(
      ((now() - old)/ window-1).toInt, N)
    if (nskip > 0) {
      val r = math.min(nskip, N-i)
      Arrays.fill(buf, i, i+r, 0L)
      Arrays.fill(buf, 0, nskip - r, 0L)
      i = (i+nskip)%N
    }

    old = now()
    gen += 1
  }

  /** Reset the state of the adder */
  def reset(): Unit = {
    Arrays.fill(buf, 0, N, 0L)
    writer.reset()
    old = now()
  }

  /** Increment the adder by 1 */
  def incr(): Unit = add(1)

  /** Increment the adder by `x` */
  def add(x: Int): Unit = {
    if ((now() - old) >= window)
      expired()
    writer.add(x)
  }

  /** Retrieve the current sum of the adder */
  def sum(): Long = {
    if ((now() - old) >= window)
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
