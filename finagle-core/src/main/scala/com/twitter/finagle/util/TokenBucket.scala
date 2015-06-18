package com.twitter.finagle.util

import com.twitter.util.Duration

/**
 * A token bucket is used to control the relative rates of two
 * processes: one fills the bucket, another empties it.
 */
private[finagle] trait TokenBucket {
  /**
   * Put `n` tokens into the bucket.
   */
  def put(n: Int): Unit

  /**
   * Try to get `n` tokens out of the bucket.
   *
   * @return true if successful
   */
  def tryGet(n: Int): Boolean

  /**
   * The number of tokens currently in the bucket.
   */
  def count: Long
}

private[finagle] object TokenBucket {
  /**
   * A leaky bucket expires tokens after approximately `ttl` time.
   * Thus, a bucket left alone will empty itself.
   *
   * @param ttl The (approximate) time after which a token will
   * expire.
   *
   * @param reserve The number of reserve tokens over the TTL
   * period. That is, every `ttl` has `reserve` tokens in addition to
   * the ones added to the bucket.

   * @param nowMs The current time in milliseconds
   */
  def newLeakyBucket(ttl: Duration, reserve: Int, nowMs: () => Long): TokenBucket = new TokenBucket {
    private[this] val w = WindowedAdder(ttl.inMilliseconds, 10, nowMs)

    def put(n: Int): Unit = w.add(n)

    def tryGet(n: Int): Boolean = synchronized {
      // Note that this is a bit sloppy: the answer to w.sum
      // can change before we're able to decrement it. That's
      // ok, though, because the debit will simply roll over to
      // the next window.
      //
      // We could also add sloppiness here: any sum > 0 we
      // can debit, but the sum just rolls over.
      val ok = count >= n
      if (ok)
        w.add(-n)
      ok
    }

    def count: Long = w.sum() + reserve
  }

  /**
   * A leaky bucket expires tokens after approximately `ttl` time.
   * Thus, a bucket left alone will empty itself.
   *
   * @param ttl The (approximate) time after which a token will
   * expire.
   *
   * @param reserve The number of reserve tokens over the TTL
   * period. That is, every `ttl` has `reserve` tokens in addition to
   * the ones added to the bucket.
   */
  def newLeakyBucket(ttl: Duration, reserve: Int): TokenBucket =
    newLeakyBucket(ttl, reserve, WindowedAdder.systemMs)
}
