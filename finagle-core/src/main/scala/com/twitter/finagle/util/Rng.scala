package com.twitter.finagle.util

import java.util.concurrent.ThreadLocalRandom

/**
 * A random number generator. Java's divergent interfaces
 * forces our hand here: ThreadLocalRandom does not conform
 * to java.util.Random. We bridge this gap.
 */
trait Rng {

  /**
   * Generate a random Double between 0.0 and 1.0, inclusive.
   */
  def nextDouble(): Double

  /**
   * Generate a random Int between 0 (inclusive) and `n` (exclusive).
   *
   * @param n the upper bound (exclusive). Must be a positive value.
   */
  def nextInt(n: Int): Int

  /**
   * Generate a random Int across the entire allowed integer values
   * from `Int.MinValue` to `Int.MaxValue`, inclusive.
   */
  def nextInt(): Int

  /**
   * Generate a random Long between 0 (inclusive) and `n` (exclusive).
   *
   * @param n the upper bound (exclusive). Must be a positive value.
   */
  def nextLong(n: Long): Long
}

/**
 * See [[Rngs]] for Java compatible APIs.
 */
object Rng {
  def apply(): Rng = Rng(new java.util.Random)
  def apply(seed: Long): Rng = Rng(new java.util.Random(seed))
  def apply(r: scala.util.Random): Rng = Rng(r.self)
  def apply(r: java.util.Random): Rng = new Rng {
    def nextDouble(): Double = r.nextDouble()
    def nextInt(n: Int): Int = r.nextInt(n)
    def nextInt(): Int = r.nextInt()
    def nextLong(n: Long): Long = {
      require(n > 0)

      // This is the algorithm used by Java's random number generator
      // internally.
      //   https://docs.oracle.com/javase/6/docs/api/java/util/Random.html#nextInt(int)
      if ((n & -n) == n)
        return r.nextLong() % n

      var bits = 0L
      var v = 0L
      do {
        bits = (r.nextLong() << 1) >>> 1
        v = bits % n
      } while (bits - v + (n - 1) < 0L)
      v
    }
  }
  val threadLocal: Rng = new Rng {
    def nextDouble(): Double = ThreadLocalRandom.current().nextDouble()
    def nextInt(n: Int): Int = ThreadLocalRandom.current().nextInt(0, n)
    def nextInt(): Int = ThreadLocalRandom.current().nextInt()
    def nextLong(n: Long): Long = ThreadLocalRandom.current().nextLong(n)
  }
}

/** Java compatible forwarders. */
object Rngs {
  val threadLocal: Rng = Rng.threadLocal
}
