package com.twitter.finagle.service

import RetryPolicy._
import com.twitter.conversions.time._
import com.twitter.finagle.{ChannelClosedException, Failure, TimeoutException, WriteException}
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RetryPolicyTest extends FunSpec {
  def getBackoffs(
    policy: RetryPolicy[Try[Nothing]],
    exceptions: Stream[Exception]
  ): Stream[Duration] =
    exceptions match {
      case Stream.Empty => Stream.empty
      case e #:: tail =>
        policy(Throw(e)) match {
          case None => Stream.empty
          case Some((backoff, p2)) => backoff #:: getBackoffs(p2, tail)
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

      assert(weo(Throw(new Exception)) == false)
      assert(weo(Throw(WriteException(new Exception))) == true)
      assert(weo(Throw(Failure(new Exception, Failure.Interrupted))) == false)
      // it's important that this failure isn't retried, despite being "restartable".
      // interrupted futures should never be retried.
      assert(weo(Throw(Failure(new Exception, Failure.Interrupted|Failure.Restartable))) == false)
      assert(weo(Throw(Failure(new Exception, Failure.Restartable))) == true)
      assert(weo(Throw(timeoutExc)) == false)
    }

    it("should TimeoutAndWriteExceptionsOnly") {
      val taweo = TimeoutAndWriteExceptionsOnly orElse NoExceptions

      assert(taweo(Throw(new Exception)) == false)
      assert(taweo(Throw(WriteException(new Exception))) == true)
      assert(taweo(Throw(Failure(new Exception, Failure.Interrupted))) == false)
      assert(taweo(Throw(Failure(timeoutExc, Failure.Interrupted))) == true)
      assert(taweo(Throw(timeoutExc)) == true)
      assert(taweo(Throw(new com.twitter.util.TimeoutException(""))) == true)
    }

    it("RetryableWriteException matches retryable exception") {
      val retryable = Seq(Failure.rejected("test"), WriteException(new Exception))
      val nonRetryable =
        Seq(Failure("test", Failure.Interrupted), new Exception, new ChannelClosedException)

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
    val backoffs = Stream(10.milliseconds, 20.milliseconds, 30.milliseconds)
    val policy = RetryPolicy.backoff(backoffs)(iExceptionsOnly).filter(iGreaterThan1)

    it("returns None if filter rejects") {
      val actual = getBackoffs(policy, Stream(IException(0), IException(1)))
      assert(actual == Stream.empty)
    }

    it("returns underlying result if filter accepts first") {
      val actual = getBackoffs(policy, Stream(IException(2), IException(0)))
      assert(actual == backoffs.take(2))
    }
  }

  describe("RetryPolicy.filterEach") {
    val backoffs = Stream(10.milliseconds, 20.milliseconds, 30.milliseconds)
    val policy = RetryPolicy.backoff(backoffs)(iExceptionsOnly).filterEach(iGreaterThan1)

    it("returns None if filterEach rejects") {
      val actual = getBackoffs(policy, Stream(IException(0), IException(1)))
      assert(actual == Stream.empty)
    }

    it("returns underlying result if filterEach accepts") {
      val actual = getBackoffs(policy, Stream(IException(2), IException(2), IException(0)))
      assert(actual == backoffs.take(2))
    }
  }

  describe("RetryPolicy.limit") {
    var currentMaxRetries: Int = 0
    val maxBackoffs = Stream.fill(3)(10.milliseconds)
    val policy =
      RetryPolicy.backoff(maxBackoffs)(RetryPolicy.ChannelClosedExceptionsOnly)
        .limit(currentMaxRetries)

    it("limits retries dynamically") {
      for (i <- 0 until 5) {
        currentMaxRetries = i
        val backoffs = getBackoffs(policy, Stream.fill(3)(new ChannelClosedException()))
        assert(backoffs == maxBackoffs.take(i min 3))
      }
    }
  }

  describe("RetryPolicy.combine") {
    val channelClosedBackoff = 10.milliseconds
    val writeExceptionBackoff = 0.milliseconds

    val combinedPolicy =
      RetryPolicy.combine(
        RetryPolicy.backoff(Backoff.const(Duration.Zero).take(2))(RetryPolicy.WriteExceptionsOnly),
        RetryPolicy.backoff(Stream.fill(3)(channelClosedBackoff))(RetryPolicy.ChannelClosedExceptionsOnly)
      )

    it("return None for unmatched exception") {
      val backoffs = getBackoffs(combinedPolicy, Stream(new UnsupportedOperationException))
      assert(backoffs == Stream.empty)
    }

    it("mimicks first policy") {
      val backoffs = getBackoffs(combinedPolicy, Stream.fill(4)(WriteException(new Exception)))
      assert(backoffs == Stream.fill(2)(writeExceptionBackoff))
    }

    it("mimicks second policy") {
      val backoffs = getBackoffs(combinedPolicy, Stream.fill(4)(new ChannelClosedException()))
      assert(backoffs == Stream.fill(3)(channelClosedBackoff))
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
      val expectedBackoffs = Stream(
        channelClosedBackoff,
        writeExceptionBackoff,
        writeExceptionBackoff,
        channelClosedBackoff
      )
      assert(backoffs == expectedBackoffs)
    }
  }
}
