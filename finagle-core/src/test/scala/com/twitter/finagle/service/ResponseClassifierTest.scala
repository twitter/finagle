package com.twitter.finagle.service

import com.twitter.finagle.{ChannelClosedException, Failure, TimeoutException}
import com.twitter.finagle.service.ResponseClass._
import com.twitter.conversions.time._
import com.twitter.util.{Return, Throw}
import org.scalatest.FunSuite

class ResponseClassifierTest extends FunSuite {
  def reqRepFromException(exception: Exception): ReqRep = ReqRep(null, Throw(exception))

  val timeoutExc = new TimeoutException {
    protected val timeout = 0.seconds
    protected val explanation = "!"
  }

  test("Default classification") {
    assert("DefaultResponseClassifier" == ResponseClassifier.Default.toString)
    assert(
      Success ==
        ResponseClassifier.Default(ReqRep(null, Return("hi")))
    )

    assert(
      RetryableFailure ==
        ResponseClassifier.Default(ReqRep(null, Throw(Failure.rejected)))
    )

    assert(
      NonRetryableFailure ==
        ResponseClassifier.Default(ReqRep(null, Throw(Failure("nope"))))
    )
  }

  test("composition") {
    val aThrow = Throw(Failure("nope"))
    val aReturn = Return("yep")

    val evens: ResponseClassifier = {
      case ReqRep(i: Int, Throw(_)) if i % 2 == 0 => RetryableFailure
    }
    val odds: ResponseClassifier = {
      case ReqRep(i: Int, Throw(_)) if i % 2 == 1 => NonRetryableFailure
    }
    val classifier = evens.orElse(odds)

    assert(RetryableFailure == classifier(ReqRep(2, aThrow)))
    assert(NonRetryableFailure == classifier(ReqRep(1, aThrow)))

    assert(!classifier.isDefinedAt(ReqRep(0, aReturn)))
    assert(Success == classifier.applyOrElse(ReqRep(0, aReturn), ResponseClassifier.Default))
  }

  test("Retry on all throws") {
    assert("RetryOnThrowsResponseClassifier" == ResponseClassifier.RetryOnThrows.toString)

    assert(
      RetryableFailure ==
        ResponseClassifier.RetryOnThrows(reqRepFromException(Failure.rejected))
    )

    assert(
      RetryableFailure ==
        ResponseClassifier.RetryOnThrows(reqRepFromException(timeoutExc))
    )
  }

  test("Retry on all Timeouts") {
    assert("RetryOnTimeoutClassifier" == ResponseClassifier.RetryOnTimeout.toString())

    val rot = ResponseClassifier.RetryOnTimeout

    assert(
      RetryableFailure ==
        rot(reqRepFromException(Failure(timeoutExc, Failure.Interrupted)))
    )

    assert(RetryableFailure == rot(reqRepFromException(timeoutExc)))

    assert(
      RetryableFailure ==
        rot(reqRepFromException(new com.twitter.util.TimeoutException("")))
    )
  }

  test("Retry on channel closed") {
    assert("RetryOnChannelClosedClassifier" == ResponseClassifier.RetryOnChannelClosed.toString())

    assert(
      RetryableFailure ==
        ResponseClassifier.RetryOnChannelClosed(reqRepFromException(new ChannelClosedException))
    )
  }
}
