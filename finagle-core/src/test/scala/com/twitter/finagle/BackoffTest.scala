package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Backoff.DecorrelatedJittered
import com.twitter.finagle.Backoff.EqualJittered
import com.twitter.finagle.Backoff.ExponentialJittered
import com.twitter.finagle.util.Rng
import com.twitter.util.Duration
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.collection.mutable.ArrayBuffer
import org.scalatest.funsuite.AnyFunSuite

class BackoffTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  test("empty") {
    val backoff: Backoff = Backoff.empty
    assert(backoff.isExhausted)
    val head = intercept[NoSuchElementException](backoff.duration)
    assert(head.getMessage == "duration of empty Backoff")
    val next = intercept[UnsupportedOperationException](backoff.next)
    assert(next.getMessage == "next of empty Backoff")
    assert(backoff.take(1) == Backoff.empty)
  }

  test("apply") {
    def f(in: Duration): Duration = in + 1.millis
    val backoff: Backoff = Backoff.apply(1.millis)(f)
    val result = Seq(1.millis, 2.millis, 3.millis, 4.millis, 5.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("fromFunction") {
    forAll { seed: Long =>
      val fRng, rng = Rng(seed)
      val f: () => Duration = () => {
        Duration.fromNanoseconds(fRng.nextLong(10))
      }
      var backoff = Backoff.fromFunction(f)
      for (_ <- 0 until 5) {
        assert(backoff.duration.inNanoseconds == rng.nextLong(10))
        backoff = backoff.next
      }
      assert(!backoff.isExhausted)
    }
  }

  test("const") {
    val backoff: Backoff = Backoff.const(7.millis)
    val result = Seq.fill(7)(7.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("exponential") {
    val backoff: Backoff = Backoff.exponential(2.millis, 2)
    val result = Seq(2.millis, 4.millis, 8.millis, 16.millis, 32.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("exponential with maximum") {
    val backoff: Backoff = Backoff.exponential(2.millis, 2, 15.millis)
    val result = Seq(2.millis, 4.millis, 8.millis, 15.millis, 15.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("linear") {
    val backoff: Backoff = Backoff.linear(7.millis, 10.millis)
    val result = Seq(7.millis, 17.millis, 27.millis, 37.millis, 47.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("linear with maximum") {
    val backoff: Backoff = Backoff.linear(9.millis, 30.millis, 99.millis)
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
        val backoff: Backoff =
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
        val backoff: Backoff =
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
        val backoff: Backoff =
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
    val backoff: Backoff = Backoff.linear(7.millis, 10.millis).take(5)
    val result = Seq(7.millis, 17.millis, 27.millis, 37.millis, 47.millis)
    verifyBackoff(backoff, result, exhausted = true)
  }

  test("take(0)") {
    val backoff = Backoff.const(1.millis).take(0)
    assert(backoff == Backoff.empty)
    assert(backoff.isExhausted)
    val head = intercept[NoSuchElementException](backoff.duration)
    assert(head.getMessage == "duration of empty Backoff")
    val next = intercept[UnsupportedOperationException](backoff.next)
    assert(next.getMessage == "next of empty Backoff")
  }

  test("takeUntil") {
    var backoff: Backoff = Backoff.linear(1.second, 1.second).takeUntil(9.seconds)
    var sumBackoff: Duration = Duration.Zero
    val backoffs: ArrayBuffer[Duration] = ArrayBuffer.empty[Duration]
    while (sumBackoff < 9.seconds) {
      assert(!backoff.isExhausted)
      sumBackoff += backoff.duration
      backoffs += backoff.duration
      backoff = backoff.next
    }
    assert(backoffs == ArrayBuffer[Duration](1.seconds, 2.seconds, 3.seconds, 4.seconds))
    assert(backoff.isExhausted)
  }

  test("takeUntil(Duration.Zero)") {
    val backoff = Backoff.const(1.millis).takeUntil(Duration.Zero)
    assert(backoff == Backoff.empty)
    assert(backoff.isExhausted)
    val head = intercept[NoSuchElementException](backoff.duration)
    assert(head.getMessage == "duration of empty Backoff")
    val next = intercept[UnsupportedOperationException](backoff.next)
    assert(next.getMessage == "next of empty Backoff")
  }

  test("concat 2 non empty Backoffs") {
    var backoff: Backoff =
      // first Backoff iterates once
      Backoff
        .const(1.millis).take(1)
        // second Backoff iterates twice
        .concat(Backoff.linear(2.millis, 1.millis).take(2))
        // third Backoff is infinite
        .concat(Backoff.const(4.millis))

    val result1 = Seq(1.millis)
    verifyBackoff(backoff, result1, exhausted = false)
    val result2 = Seq(2.millis, 3.millis)
    backoff = backoff.next
    verifyBackoff(backoff, result2, exhausted = false)
    val result3 = Seq(4.millis, 4.millis, 4.millis)
    backoff = backoff.next.next
    verifyBackoff(backoff, result3, exhausted = false)
  }

  test("concat a non-empty Backoff and an empty Backoff") {
    val backoff: Backoff =
      Backoff.linear(1.millis, 1.millis).take(5).concat(Backoff.empty)
    val result = Seq(1.millis, 2.millis, 3.millis, 4.millis, 5.millis)
    verifyBackoff(backoff, result, exhausted = true)
  }

  test("concat an empty Backoff and a non empty Backoff") {
    val backoff: Backoff =
      Backoff.empty.concat(Backoff.linear(1.millis, 1.millis))
    val result = Seq(1.millis, 2.millis, 3.millis, 4.millis, 5.millis)
    verifyBackoff(backoff, result, exhausted = false)
  }

  test("concat 2 empty Backoffs") {
    val backoff: Backoff = Backoff.empty.concat(Backoff.empty)
    assert(backoff == Backoff.empty)
  }

  test("++ as an alias of concat") {
    val backoff: Backoff = Backoff.const(1.second).take(3) ++
      Backoff.linear(2.seconds, 1.second).take(2) ++
      Backoff.const(7.seconds).take(3)
    val result = Seq(1, 1, 1, 2, 3, 7, 7, 7).map(Duration.fromSeconds)
    verifyBackoff(backoff, result, exhausted = true)
  }

  test("toStream") {
    var backoff = Backoff.linear(1.second, 5.seconds).take(10)
    var stream = backoff.toStream
    while (!backoff.isExhausted) {
      assert(backoff.duration == stream.head)
      backoff = backoff.next
      stream = stream.tail
    }
    assert(backoff.isExhausted)
    assert(stream.isEmpty)
  }

  test("toStream from an empty Backoff") {
    val emptyStream = Backoff.empty.toStream
    assert(emptyStream.isEmpty)
    val head = intercept[NoSuchElementException](emptyStream.head)
    assert(head.getMessage == "head of empty stream")
    val next = intercept[UnsupportedOperationException](emptyStream.tail)
    assert(next.getMessage == "tail of empty stream")
  }

  test("toJavaStream") {
    var backoff = Backoff.linear(1.second, 1.second).take(10)
    val streamIterator = backoff.toJavaIterator
    while (!backoff.isExhausted) {
      assert(backoff.duration == streamIterator.next())
      backoff = backoff.next
    }
    assert(backoff.isExhausted)
    assert(!streamIterator.hasNext)
  }

  test("fromStream") {
    var stream = Stream(1.second, 9.seconds, 3.seconds)
    var backoff = Backoff.fromStream(stream)
    while (!backoff.isExhausted) {
      assert(backoff.duration == stream.head)
      backoff = backoff.next
      stream = stream.tail
    }
    assert(backoff.isExhausted)
    assert(stream.isEmpty)
  }

  test("fromStream with an empty Backoff") {
    val emptyBackoff = Backoff.fromStream(Stream.empty[Duration])
    assert(emptyBackoff.isExhausted)
    val head = intercept[NoSuchElementException](emptyBackoff.duration)
    assert(head.getMessage == "duration of empty Backoff")
    val next = intercept[UnsupportedOperationException](emptyBackoff.next)
    assert(next.getMessage == "next of empty Backoff")
  }

  test("fromStream with Stream created from Backoff#toStream") {
    var stream = Backoff.exponential(2.seconds, 3).toStream.take(10)
    var backoff = Backoff.fromStream(stream)
    while (!backoff.isExhausted) {
      assert(backoff.duration == stream.head)
      backoff = backoff.next
      stream = stream.tail
    }
    assert(backoff.isExhausted)
    assert(stream.isEmpty)
  }

  private[this] def verifyBackoff(
    backoff: Backoff,
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
