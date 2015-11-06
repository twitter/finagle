package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.util.{Stopwatch, Time, Duration}
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RetryBudgetTest extends FunSuite
  with Matchers
{

  test("Empty") {
    val rb = RetryBudget.Empty
    assert(0 == rb.balance)
    assert(!rb.tryWithdraw())

    rb.deposit()
    assert(0 == rb.balance)
    assert(!rb.tryWithdraw())
  }

  test("Infinite") {
    val rb = RetryBudget.Infinite
    assert(100L == rb.balance)
    assert(rb.tryWithdraw())

    rb.deposit()
    assert(100 == rb.balance)
    assert(rb.tryWithdraw())
  }

  def newRetryBudget(
    ttl: Duration = 60.seconds,
    minRetriesPerSec: Int = 0,
    maxPercentOver: Double = 0.0
  ): RetryBudget =
    RetryBudget(ttl, minRetriesPerSec, maxPercentOver, Stopwatch.timeMillis)

  def testBudget(
    ttl: Duration = 60.seconds,
    minRetriesPerSec: Int = 0,
    percentCanRetry: Double = 0.0
  ): Unit = withClue(s"percentCanRetry=$percentCanRetry:") {
    // freeze time to simplify the tests
    Time.withCurrentTimeFrozen { _ =>
      val rb = newRetryBudget(ttl, minRetriesPerSec, percentCanRetry)

      // check initial conditions
      val minRetries = ttl.inSeconds * minRetriesPerSec
      assert(minRetries == rb.balance)
      if (minRetries == 0)
        assert(!rb.tryWithdraw())

      // use a decent sized number so we see less effects from fractions
      val nReqs = 10000
      var retried = 0
      0.until(nReqs).foreach { i =>
        withClue(s"request $i:") {
          rb.deposit()
          if (rb.tryWithdraw())
            retried += 1
          else
            assert(0 == rb.balance)
        }
      }

      // because TokenRetryBudget.ScaleFactor doesn't give us
      // perfect precision, we give ourselves a small error margin.
      val expectedRetries = (nReqs * percentCanRetry).toInt + minRetries
      expectedRetries should be (retried +- 1)
    }
  }

  test("apply ttl bounds check") {
    intercept[IllegalArgumentException] {
      RetryBudget(Duration.Zero, 5, 0.1)
    }
    intercept[IllegalArgumentException] {
      RetryBudget(Duration.fromSeconds(61), 5, 0.1)
    }
  }

  test("apply minRetriesPerSec bounds check") {
    intercept[IllegalArgumentException] {
      RetryBudget(Duration.fromSeconds(10), -1, 0.1)
    }
  }

  test("apply maxPercentOver bounds") {
    intercept[IllegalArgumentException] {
      RetryBudget(Duration.fromSeconds(10), 0, -1.0)
    }
  }

  test("apply minRetriesPerSec=0 maxPercentOver=0 should be empty") {
    val rb = RetryBudget(Duration.fromSeconds(10), 0, 0.0)
    assert(rb == RetryBudget.Empty)
  }

  test("apply minRetriesPerSec=0") {
    // every 10 reqs should give 1 retry
    testBudget(percentCanRetry = 0.1)

    // every 2 reqs should give 1 retry
    testBudget(percentCanRetry = 0.5)

    // every 4 reqs should give 1 retry
    testBudget(percentCanRetry = 0.25)

    // every 4 reqs should give 3 retries
    testBudget(percentCanRetry = 0.75)

    // high and and low percentages
    testBudget(percentCanRetry = 0.99)
    testBudget(percentCanRetry = 0.999)
    testBudget(percentCanRetry = 0.9999)
    testBudget(percentCanRetry = 0.01)
    testBudget(percentCanRetry = 0.001)
    testBudget(percentCanRetry = 0.0001)
  }

  test("apply minRetries=0 maxPercentOver greater than 1.0") {
    // Slightly simpler than `testBudgetNoMin` in that it runs all
    // the `request`s up front then checks the end-state.
    val percent = 2.0
    val rb = newRetryBudget(maxPercentOver = percent)

    // check initial conditions
    assert(0 == rb.balance)
    assert(!rb.tryWithdraw())

    val nReqs = 10000
    0.until(nReqs).foreach { _ =>
      rb.deposit()
    }

    val expectedRetries = (nReqs * percent).toInt
    assert(expectedRetries == rb.balance)
  }

  test("apply with minRetries and maxPercentOver=0") {
    testBudget(ttl = 1.second, minRetriesPerSec = 10, percentCanRetry = 0.0)
    testBudget(ttl = 2.second, minRetriesPerSec = 5, percentCanRetry = 0.0)
  }

  test("apply with minRetries and maxPercentOver") {
    testBudget(ttl = 1.second, minRetriesPerSec = 10, percentCanRetry = 0.1)
    testBudget(ttl = 2.second, minRetriesPerSec = 5, percentCanRetry = 0.5)
  }

}
