package com.twitter.finagle.util

import com.twitter.util.Promise
import com.twitter.concurrent.Serialized
import java.util.concurrent.BrokenBarrierException

/**
 * The AsyncCountingLatch is a kind of barrier. It operates asynchronously,
 * meaning the barrier actions are invoked when the final process enters
 * the barrier.
 *
 * This class is rather generic. It can be used like a CountDownLatch if
 * you set the intialCount to something > 0 and never call incr().
 *
 * It can also be used a reference counter by setting the initialCount
 * to zero and calling incr() and decr() as references to resources
 * are created and destroyed.
 *
 * ''Note'': This Latch is cyclic in the sense that it can be re-used.
 * Any time intialCount + |incr()| + |decr()| == 0 the barrier is triggered.
 */
class AsyncCountingLatch(initialCount: Int = 0) extends Serialized {
  require(initialCount >= 0, "initialCount must be greater than or equal to zero")

  private[this] var _count = initialCount
  @volatile private[this] var promise = new Promise[Unit]

  /**
   * Enqueues the callback for execution when the latch's count reaches zero.
   *
   * ''Note'': this function will only be invoked ONCE. After opening the
   * gate, if the latch is reset (via reset() or incr()) the callback will
   * discarded.
   */
  def await(f: => Unit) = promise ensure f

  /**
   * Returns a Future[Unit] that is satisfied when the count of this latch has
   * reached 0. Note that every
   */
  def zeros = promise

  /**
   * Increment the latch.
   */
  def incr() = synchronized { _count += 1 }

  def reset() = synchronized {
    _count = initialCount
    promise.setException(new BrokenBarrierException)
    promise = new Promise[Unit]
  }

  /**
   * Decrement the latch. If the latch value reaches 0, awaiting
   * computations are executed inline.
   */
  def decr() {
    println("in decr; ", _count)
    synchronized {
      require(_count > 0)

      _count -= 1
      println(">>> count after decr: ", _count)
      if (_count == 0) {
        println("setting value")
        promise.setValue(())
      }
    }
  }

  def count = _count
}

/**
 * An asynchrounous version of the beloved java.util.concurrent.CountDownLatch.
 * Like its ancestor this is non-cyclic (i.e., the gate can only open once).
 */
class AsyncCountDownLatch(initialCount: Int) {
  private[this] val countingLatch = new AsyncCountingLatch(initialCount)
  val zeros = countingLatch.zeros

  def await(f: => Unit) = countingLatch.await(f)

  def countDown() {
    countingLatch.decr()
  }
}