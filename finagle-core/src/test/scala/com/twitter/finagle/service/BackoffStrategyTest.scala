package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.service.BackoffStrategy.{
  DecorrelatedJittered,
  EqualJittered,
  ExponentialJittered
}
import com.twitter.finagle.util.Rng
import com.twitter.util.Duration
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.collection.mutable.ArrayBuffer

class BackoffStrategyTest extends FunSuite with ScalaCheckDrivenPropertyChecks {
  test("empty") {
    val backoff: BackoffStrategy = BackoffStrategy.empty
    assert(backoff.isExhausted)
    val head = intercept[NoSuchElementException](backoff.duration)
    assert(head.getMessage == "duration of an empty Backoff")
    val next = intercept[UnsupportedOperationException](backoff.next)
    assert(next.getMessage == "next of an empty Backoff")
    assert(backoff.take(1) == BackoffStrategy.empty)
  }

  test("apply") {
    def f(in: Duration): Duration = in + 1.millis
    val backoff: BackoffStrategy = BackoffStrategy.apply(1.millis, f)
    val result = Seq(1.millis, 2.millis, 3.millis, 4.millis, 5.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("fromFunction") {
    forAll { seed: Long =>
      val fRng, rng = Rng(seed)
      val f: () => Duration = () => {
        Duration.fromNanoseconds(fRng.nextLong(10))
      }
      var backoff = BackoffStrategy.fromFunction(f)
      for (_ <- 0 until 5) {
        assert(backoff.duration.inNanoseconds == rng.nextLong(10))
        backoff = backoff.next
      }
      assert(!backoff.isExhausted)
    }
  }

  test("const") {
    val backoff: BackoffStrategy = BackoffStrategy.const(7.millis)
    val result = Seq.fill(7)(7.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("exponential") {
    val backoff: BackoffStrategy = BackoffStrategy.exponential(2.millis, 2)
    val result = Seq(2.millis, 4.millis, 8.millis, 16.millis, 32.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("exponential with maximum") {
    val backoff: BackoffStrategy = BackoffStrategy.exponential(2.millis, 2, 15.millis)
    val result = Seq(2.millis, 4.millis, 8.millis, 15.millis, 15.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("linear") {
    val backoff: BackoffStrategy = BackoffStrategy.linear(7.millis, 10.millis)
    val result = Seq(7.millis, 17.millis, 27.millis, 37.millis, 47.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("linear with maximum") {
    val backoff: BackoffStrategy = BackoffStrategy.linear(9.millis, 30.millis, 99.millis)
    val result = Seq(9.millis, 39.millis, 69.millis, 99.millis, 99.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("decorrelatedJittered") {
    val decorrelatedGen = for {
      startMs <- Gen.choose(1L, 1000L)
      maxMs <- Gen.choose(startMs, startMs * 2)
      seed <- Gen.choose(Long.MinValue, Long.MaxValue)
    } yield (startMs, maxMs, seed)

    forAll(decorrelatedGen) {
      case (startMs: Long, maxMs: Long, seed: Long) =>
        val rng = Rng(seed)
        val backoff: BackoffStrategy =
          new DecorrelatedJittered(startMs.millis, maxMs.millis, Rng(seed))
        val result: ArrayBuffer[Duration] = new ArrayBuffer[Duration]()
        var start = startMs.millis
        for (_ <- 1 to 5) {
          result.append(start)
          start = nextStart(start, maxMs.millis, rng)
        }
        verifyBackoff(backoff, result.toSeq, exhausted = false)
    }

    def nextStart(start: Duration, maximum: Duration, rng: Rng): Duration = {
      // in case of Long overflow
      val upperbound = if (start >= maximum / 3) maximum else start * 3
      val randRange = math.abs(upperbound.inNanoseconds - start.inNanoseconds)
      if (randRange == 0) start
      else Duration.fromNanoseconds(start.inNanoseconds + rng.nextLong(randRange))
    }
  }

  test("equalJittered") {
    val equalGen = for {
      startMs <- Gen.choose(1L, 1000L)
      maxMs <- Gen.choose(startMs, startMs * 2)
      seed <- Gen.choose(Long.MinValue, Long.MaxValue)
    } yield (startMs, maxMs, seed)

    forAll(equalGen) {
      case (startMs: Long, maxMs: Long, seed: Long) =>
        val rng = Rng(seed)
        val backoff: BackoffStrategy =
          new EqualJittered(startMs.millis, startMs.millis, maxMs.millis, 1, Rng(seed))
        val result: ArrayBuffer[Duration] = new ArrayBuffer[Duration]()
        var start = startMs.millis
        for (attempt <- 1 to 7) {
          result.append(start)
          start = nextStart(startMs.millis, maxMs.millis, rng, attempt)
        }
        verifyBackoff(backoff, result.toSeq, exhausted = false)
    }

    def nextStart(start: Duration, maximum: Duration, rng: Rng, attempt: Int): Duration = {
      val shift = 1L << (attempt - 1)
      // in case of Long overflow
      val halfExp = if (start >= maximum / shift) maximum else start * shift
      val randomBackoff = Duration.fromNanoseconds(rng.nextLong(halfExp.inNanoseconds))

      // in case of Long overflow
      if (halfExp == maximum || halfExp >= maximum - randomBackoff) maximum
      else halfExp + randomBackoff
    }
  }

  test("exponentialJittered") {
    val exponentialGen = for {
      startMs <- Gen.choose(1L, 1000L)
      maxMs <- Gen.choose(startMs, startMs * 2)
      seed <- Gen.choose(Long.MinValue, Long.MaxValue)
    } yield (startMs, maxMs, seed)

    forAll(exponentialGen) {
      case (startMs: Long, maxMs: Long, seed: Long) =>
        val rng = Rng(seed)
        val backoff: BackoffStrategy =
          new ExponentialJittered(startMs.millis, maxMs.millis, 1, Rng(seed))
        val result: ArrayBuffer[Duration] = new ArrayBuffer[Duration]()
        var start = startMs.millis
        for (attempt <- 1 to 7) {
          result.append(start)
          start = nextStart(start, maxMs.millis, rng, attempt)
        }
        verifyBackoff(backoff, result.toSeq, exhausted = false)
    }

    def nextStart(start: Duration, max: Duration, rng: Rng, attempt: Int): Duration = {
      val shift = 1L << attempt
      // to avoid Long overflow
      val maxBackoff = if (start >= max / shift) max else start * shift
      if (maxBackoff == max) max
      else Duration.fromNanoseconds(rng.nextLong(maxBackoff.inNanoseconds))
    }
  }

  test("take") {
    val linearBackoff: BackoffStrategy = BackoffStrategy.linear(7.millis, 10.millis)
    val backoff: BackoffStrategy = linearBackoff.take(5)
    val result = Seq(7.millis, 17.millis, 27.millis, 37.millis, 47.millis)
    verifyBackoff(backoff, result, exhausted = true)
  }

  test("take(0)") {
    val backoff = BackoffStrategy.const(1.millis).take(0)
    assert(backoff == BackoffStrategy.empty)
    assert(backoff.isExhausted)
    val head = intercept[NoSuchElementException](backoff.duration)
    assert(head.getMessage == "duration of an empty Backoff")
    val next = intercept[UnsupportedOperationException](backoff.next)
    assert(next.getMessage == "next of an empty Backoff")
  }

  test("concat 2 non empty strategies") {
    var backoff: BackoffStrategy =
      // first strategy iterates once
      BackoffStrategy
        .const(1.millis).take(1)
        // second strategy iterates twice
        .concat(BackoffStrategy.linear(2.millis, 1.millis).take(2))
        // third strategy is infinite
        .concat(BackoffStrategy.const(4.millis))

    val result1 = Seq(1.millis)
    verifyBackoff(backoff, result1, exhausted = false)
    val result2 = Seq(2.millis, 3.millis)
    backoff = backoff.next
    verifyBackoff(backoff, result2, exhausted = false)
    val result3 = Seq(4.millis, 4.millis, 4.millis)
    backoff = backoff.next.next
    verifyBackoff(backoff, result3, exhausted = false)
  }

  test("concat a non-empty strategy and an empty strategy") {
    val backoff: BackoffStrategy =
      BackoffStrategy.linear(1.millis, 1.millis).take(5).concat(BackoffStrategy.empty)
    val result = Seq(1.millis, 2.millis, 3.millis, 4.millis, 5.millis)
    verifyBackoff(backoff, result, exhausted = true)
  }

  test("concat an empty strategy and a non empty strategy") {
    val backoff: BackoffStrategy =
      BackoffStrategy.empty.concat(BackoffStrategy.linear(1.millis, 1.millis))
    val result = Seq(1.millis, 2.millis, 3.millis, 4.millis, 5.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("concat 2 empty strategies") {
    val backoff: BackoffStrategy = BackoffStrategy.empty.concat(BackoffStrategy.empty)
    assert(backoff == BackoffStrategy.empty)
  }

  private[this] def verifyBackoff(
    backoff: BackoffStrategy,
    result: Seq[Duration],
    exhausted: Boolean
  ): Unit = {
    var actualBackoff = backoff
    result.foreach { expectedBackoff =>
      assert(actualBackoff.duration == expectedBackoff)
      actualBackoff = actualBackoff.next
    }
    assert(actualBackoff.isExhausted == exhausted)
  }
}
