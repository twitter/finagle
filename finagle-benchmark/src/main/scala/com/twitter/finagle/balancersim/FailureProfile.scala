package com.twitter.finagle.balancersim

import com.twitter.util.Duration
import com.twitter.util.Stopwatch
import scala.io.Source
import scala.util.Random

private object FailureProfile {
  val rng: Random = new Random("seed".hashCode)

  /** Create a failure profile using this instantiation as the start time */
  def apply(): FailureProfile = new FailureProfile(Stopwatch.start())

  /** Fulfill all requests successfully */
  val alwaysSucceed: () => Boolean = () => false

  /** Fail all requests */
  val alwaysFail: () => Boolean = () => true

  /**
   * Creates a failure profile from a file where each line is a boolean
   * representing a response success or failure. One of these responses is
   * picked uniformly at random as a starting point for the profile. Retrieving
   * the next response type is synchronized across threads
   */
  def fromFile(path: java.net.URL): () => Boolean = {
    val responses =
      Source.fromURL(path).getLines.toIndexedSeq.map { line: String => line.toBoolean }

    () => responses(rng.nextInt(responses.size))
  }
}

/**
 * Creates a profile to determine whether the next incoming request should be
 * a success or failure based on the elapsed time.
 */
private class FailureProfile(elapsed: () => Duration) {

  /** Successfully fulfill requests within the healthy period but fail all else. */
  def failAfter(healthyPeriod: Duration): () => Boolean = () => {
    val timeElapsed = elapsed()

    timeElapsed >= healthyPeriod
  }

  /** Fail all requests within min and max, inclusive. Succeed otherwise. */
  def failWithin(min: Duration, max: Duration): () => Boolean = () => {
    val timeElapsed = elapsed()

    timeElapsed >= min && timeElapsed <= max
  }
}
