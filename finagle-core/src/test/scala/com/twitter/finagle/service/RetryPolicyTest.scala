package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.service.RetryPolicy._
import com.twitter.finagle.{
  Backoff,
  ChannelClosedException,
  Failure,
  FailureFlags,
  TimeoutException,
  WriteException
}
import com.twitter.util._
import org.scalatest.funspec.AnyFunSpec

class RetryPolicyTest extends AnyFunSpec {

  def getBackoffs(
    policy: RetryPolicy[Try[Nothing]],
    exceptions: Stream[Exception]
  ): Backoff = {
    exceptions match {
      case Stream.Empty => Backoff.empty
      case e #:: tail =>
        policy(Throw(e)) match {
          case None => Backoff.empty
          case Some((backoff, p2)) => Backoff.fromStream(backoff #:: getBackoffs(p2, tail).toStream)
        }
    }
  }

  describe("RetryPolicy") {
    val NoExceptions: PartialFunction[Try[Nothing], Boolean] = {
      case _ => false
    }
    val timeoutExc = new TimeoutException {
      protected val timeout = 0.seconds
      protected val explanation = "!"
    }

    it("should WriteExceptionsOnly") {
      val weo = WriteExceptionsOnly orElse NoExceptions

      assert(!weo(Throw(new Exception)))
      assert(weo(Throw(WriteException(new Exception))))
      assert(!weo(Throw(Failure(new Exception, FailureFlags.Interrupted))))
      // it's important that this failure isn't retried, despite being "retryable".
      // interrupted futures should never be retried.
      assert(!weo(Throw(Failure(new Exception, FailureFlags.Interrupted | FailureFlags.Retryable))))
      assert(weo(Throw(Failure(new Exception, FailureFlags.Retryable))))
      assert(!weo(Throw(Failure(new Exception, FailureFlags.Rejected | FailureFlags.NonRetryable))))
      assert(!weo(Throw(timeoutExc)))
    }

    it("should TimeoutAndWriteExceptionsOnly") {
      val taweo = TimeoutAndWriteExceptionsOnly orElse NoExceptions

      assert(!taweo(Throw(new Exception)))
      assert(taweo(Throw(WriteException(new Exception))))
      assert(!taweo(Throw(Failure(new Exception, FailureFlags.Interrupted))))
      assert(taweo(Throw(Failure(timeoutExc, FailureFlags.Interrupted))))
      assert(taweo(Throw(timeoutExc)))
      assert(taweo(Throw(new com.twitter.util.TimeoutException(""))))
    }

    it("RetryableWriteException matches retryable exception") {
      val retryable = Seq(Failure.rejected("test"), WriteException(new Exception))
      val nonRetryable =
        Seq(
          Failure("test", FailureFlags.Interrupted),
          new Exception,
          new ChannelClosedException,
          Failure("boo", FailureFlags.NonRetryable)
        )

      retryable.foreach {
        case RetryPolicy.RetryableWriteException(_) =>
        case _ => fail("should match RetryableWriteException")
      }

      nonRetryable.foreach {
        case RetryPolicy.RetryableWriteException(_) =>
          fail("should not match RetryableWriteException")
        case _ =>
      }
    }
  }

  case class IException(i: Int) extends Exception

  val iExceptionsOnly: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(IException(_)) => true
  }

  val iGreaterThan1: Try[Nothing] => Boolean = {
    case Throw(IException(i)) if i > 1 => true
    case _ => false
  }

  describe("RetryPolicy.filter/filterEach") {
    val backoffs = Backoff.linear(10.milliseconds, 10.milliseconds).take(3)
    val policy = RetryPolicy.backoff(backoffs)(iExceptionsOnly).filter(iGreaterThan1)

    it("returns None if filter rejects") {
      val actual = getBackoffs(policy, Stream(IException(0), IException(1)))
      assert(actual == Backoff.empty)
    }

    it("returns underlying result if filter accepts first") {
      val actual = getBackoffs(policy, Stream(IException(2), IException(0)))
      verifyBackoff(actual, backoffs.take(2))
    }
  }

  describe("RetryPolicy.filterEach") {
    val backoffs = Backoff.linear(10.milliseconds, 10.milliseconds).take(3)
    val policy = RetryPolicy.backoff(backoffs)(iExceptionsOnly).filterEach(iGreaterThan1)

    it("returns None if filterEach rejects") {
      val actual = getBackoffs(policy, Stream(IException(0), IException(1)))
      assert(actual == Backoff.empty)
    }

    it("returns underlying result if filterEach accepts") {
      val actual = getBackoffs(policy, Stream(IException(2), IException(2), IException(0)))
      verifyBackoff(actual, backoffs.take(2))
    }
  }

  describe("RetryPolicy.limit") {
    var currentMaxRetries: Int = 0
    val maxBackoffs = Backoff.const(10.milliseconds).take(3)
    val policy =
      RetryPolicy
        .backoff(maxBackoffs)(RetryPolicy.ChannelClosedExceptionsOnly)
        .limit(currentMaxRetries)

    it("limits retries dynamically") {
      for (i <- 0 until 5) {
        currentMaxRetries = i
        val backoffs = getBackoffs(policy, Stream.fill(3)(new ChannelClosedException()))
        verifyBackoff(backoffs, maxBackoffs.take(1.min(3)))
      }
    }
  }

  describe("RetryPolicy.combine") {
    val channelClosedBackoff = 10.milliseconds
    val writeExceptionBackoff = 0.milliseconds

    val combinedPolicy =
      RetryPolicy.combine(
        RetryPolicy.backoff(Backoff.const(Duration.Zero).take(2))(RetryPolicy.WriteExceptionsOnly),
        RetryPolicy
          .backoff(Backoff.const(channelClosedBackoff).take(3))(
            RetryPolicy.ChannelClosedExceptionsOnly)
      )

    it("return None for unmatched exception") {
      val backoffs = getBackoffs(combinedPolicy, Stream(new UnsupportedOperationException))
      assert(backoffs == Backoff.empty)
    }

    it("mimicks first policy") {
      val backoffs = getBackoffs(combinedPolicy, Stream.fill(4)(WriteException(new Exception)))
      verifyBackoff(backoffs, Backoff.const(writeExceptionBackoff).take(2))
    }

    it("mimicks second policy") {
      val backoffs = getBackoffs(combinedPolicy, Stream.fill(4)(new ChannelClosedException()))
      verifyBackoff(backoffs, Backoff.const(channelClosedBackoff).take(3))
    }

    it("interleaves backoffs") {
      val exceptions = Stream(
        new ChannelClosedException(),
        WriteException(new Exception),
        WriteException(new Exception),
        new ChannelClosedException(),
        WriteException(new Exception)
      )

      val backoffs = getBackoffs(combinedPolicy, exceptions)
      val expectedBackoffs = Backoff
        .const(channelClosedBackoff).take(1)
        .concat(Backoff.const(writeExceptionBackoff).take(2))
        .concat(Backoff.const(channelClosedBackoff).take(1))
      verifyBackoff(backoffs, expectedBackoffs)
    }
  }

  describe("RetryPolicy.namedPF") {
    it("uses the name parameter as the toString method") {
      val f = RetryPolicy.namedPF[Int]("foo") { case _ => false }
      assert(f.toString == "foo")
    }

    it("preserves the behavior of the underlying partial function") {
      val f: PartialFunction[Int, Boolean] = { case i if i >= 0 => true }
      val f1 = RetryPolicy.namedPF("foo")(f)

      assert(f.isDefinedAt(1) == f1.isDefinedAt(1))
      assert(f.isDefinedAt(-1) == f1.isDefinedAt(-1))
    }

    it("preserves toString information when composition with .orElse") {
      val f1 = RetryPolicy.namedPF[Int]("foo") { case i if i >= 0 => false }
      val f2 = RetryPolicy.namedPF[Int]("bar") { case _ => true }

      val composed = f1.orElse(f2)
      assert(composed.toString == "foo.orElse(bar)")
    }
  }

  describe("RetryPolicy.Never") {
    val never = RetryPolicy.Never.asInstanceOf[RetryPolicy[Try[Int]]]
    it("should not retry") {
      assert(None == never(Return(1)))
      assert(None == never(Throw(new RuntimeException)))
    }
  }

  describe("RetryPolicy.none") {
    val nah = RetryPolicy.none
    it("should not retry") {
      assert(None == nah((1, Return(1))))
      assert(None == nah((1, Throw(new RuntimeException))))
    }
  }

  private def verifyBackoff(b1: Backoff, b2: Backoff): Unit = {
    if (!b1.isExhausted && !b2.isExhausted) {
      assert(b1.duration == b2.duration)
      verifyBackoff(b1.next, b2.next)
    }
  }
}
