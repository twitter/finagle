package com.twitter.finagle.util

// When we can rely on JDK7, switch to the ThreadLocalRandom
// distributed there.
import scala.concurrent.forkjoin.ThreadLocalRandom
//import java.util.concurrent.ThreadLocalRandom

/**
 * A random number generator. Java's divergent interfaces
 * forces our hand here: ThreadLocalRandom does not conform
 * to java.util.Random. We bridge this gap.
 */
trait Rng {
  def nextDouble(): Double
  def nextInt(n: Int): Int
}

object Rng {
  def apply(): Rng = Rng(new java.util.Random)
  def apply(seed: Long): Rng = Rng(new java.util.Random(seed))
  def apply(r: scala.util.Random): Rng = Rng(r.self)
  def apply(r: java.util.Random): Rng = new Rng {
    def nextDouble(): Double = r.nextDouble()
    def nextInt(n: Int): Int = r.nextInt(n)
  }
  val threadLocal: Rng = new Rng {
    def nextDouble(): Double = ThreadLocalRandom.current().nextDouble()
    def nextInt(n: Int): Int = ThreadLocalRandom.current().nextInt(0, n)
  }
}
