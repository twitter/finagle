package com.twitter.finagle.serverset2

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Backoff
import com.twitter.finagle.Backoff.DecorrelatedJittered
import com.twitter.finagle.util.Rng
import com.twitter.util.Duration
import org.scalatest.funsuite.AnyFunSuite

class RetryStreamTest extends AnyFunSuite {

  test("RetryStream never ends") {
    // create a stream with only 2 elements. Make sure we can enumerate 100 entries
    val test = new RetryStream(Backoff.constant(Duration.Zero).take(2))
    1.to(100).foreach { _ => test.next() }
  }

  test("RetryStream reset works correctly") {
    // create a stream with only 2 elements. Make sure we can enumerate 100 entries
    val backoff = new DecorrelatedJittered(10.seconds, 10.seconds, Rng(1L))
    val test = new RetryStream(backoff)
    val first100 = 1
      .to(100)
      .map { _ => test.next() }
      .toList
    test.reset()
    val second100 = 1
      .to(100)
      .map { _ => test.next() }
      .toList
    assert(first100 == second100)
  }
}
