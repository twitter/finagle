package com.twitter.finagle.serverset2

import com.twitter.finagle.service.Backoff
import com.twitter.util.Duration
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RetryStreamTest extends FunSuite {

  test("RetryStream never ends") {
    // create a stream with only 2 elements. Make sure we can enumerate 100 entries
    val test = new RetryStream(Backoff.constant(Duration.Zero).take(2))
    1.to(100).foreach { _ => test.next() }
  }

  test("RetryStream reset works correctly") {
    // create a stream with only 2 elements. Make sure we can enumerate 100 entries
    val test = RetryStream()
    val first100 = 1.to(100).map { _ => test.next() }.toList
    test.reset()
    val second100 = 1.to(100).map { _ => test.next() }.toList
    assert(first100 == second100)
  }
}