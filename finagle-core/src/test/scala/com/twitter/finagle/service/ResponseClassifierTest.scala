package com.twitter.finagle.service

import com.twitter.finagle.{
  ChannelClosedException,
  Failure,
  FailureFlags,
  IndividualRequestTimeoutException,
  TimeoutException
}
import com.twitter.finagle.service.ResponseClass._
import com.twitter.conversions.DurationOps._
import com.twitter.util.{Duration, Return, Throw}
import org.scalatest.funsuite.AnyFunSuite

class ResponseClassifierTest extends AnyFunSuite {
  def reqRepFromException(exception: Exception): ReqRep = ReqRep(null, Throw(exception))

  val timeoutExc = new TimeoutException {
    protected val timeout: Duration = 0.seconds
    protected val explanation: String = "!"
  }

  test("named") {
    val rc1 = ResponseClassifier.named("rc1") {
      case _ => ResponseClass.Success
    }
    val rc2 = ResponseClassifier.named("rc2") {
      case _ => ResponseClass.Success
    }
    val rc3 = ResponseClassifier.named("rc3") {
      case _ => ResponseClass.Success
    }
    assert("rc1" == rc1.toString)
    assert("rc1.orElse(rc2)" == rc1.orElse(rc2).toString)
    assert("rc1.orElse(rc2).orElse(rc3)" == rc1.orElse(rc2).orElse(rc3).toString)
    assert("rc1.orElse(rc2.orElse(rc3))" == rc1.orElse(rc2.orElse(rc3)).toString)
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

    assert(
      Ignorable ==
        ResponseClassifier.Default(ReqRep(null, Throw(Failure.ignorable("ignore"))))
    )

    assert(
      NonRetryableFailure ==
        ResponseClassifier.Default(
          ReqRep(null, Throw(new IndividualRequestTimeoutException(1.second)))
        )
    )
  }

  // This is a copy of 'Default classification' with a slight modification for the
  // change in behavior towards IndividualRequestTimeoutExceptions for `IgnoreIRTEs`.
  test("IgnoreIRTEs classification") {
    assert("IgnoreIRTEsResponseClassifier" == ResponseClassifier.IgnoreIRTEs.toString)
    assert(
      Success ==
        ResponseClassifier.IgnoreIRTEs(ReqRep(null, Return("hi")))
    )

    assert(
      RetryableFailure ==
        ResponseClassifier.IgnoreIRTEs(ReqRep(null, Throw(Failure.rejected)))
    )

    assert(
      NonRetryableFailure ==
        ResponseClassifier.IgnoreIRTEs(ReqRep(null, Throw(Failure("nope"))))
    )

    assert(
      Ignorable ==
        ResponseClassifier.IgnoreIRTEs(ReqRep(null, Throw(Failure.ignorable("ignore"))))
    )

    assert(
      Ignorable ==
        ResponseClassifier.IgnoreIRTEs(
          ReqRep(null, Throw(new IndividualRequestTimeoutException(1.second)))
        )
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

    // ReqRep is weakly typed, so it will match a ReqRep or ReqRepT
    assert(RetryableFailure == classifier(ReqRep(2, aThrow)))
    assert(RetryableFailure == classifier(ReqRepT(2, aThrow)))
    assert(NonRetryableFailure == classifier(ReqRep(1, aThrow)))
    assert(NonRetryableFailure == classifier(ReqRepT(1, aThrow)))

    assert(!classifier.isDefinedAt(ReqRep(0, aReturn)))
    assert(!classifier.isDefinedAt(ReqRepT(0, aReturn)))
    assert(Success == classifier.applyOrElse(ReqRep(0, aReturn), ResponseClassifier.Default))
    assert(Success == classifier.applyOrElse(ReqRepT(0, aReturn), ResponseClassifier.Default))

  }

  test("typed composition") {
    val aThrow = Throw(Failure("nope"))
    val aReturn = Return(4)

    val evens: ResponseClassifier = {
      case ReqRepT(i: Int, Throw(_)) if i % 2 == 0 => RetryableFailure
    }
    val odds: ResponseClassifier = {
      case ReqRepT(i: Int, Throw(_)) if i % 2 == 1 => NonRetryableFailure
    }
    val classifier = evens.orElse(odds)

    assert(RetryableFailure == classifier(ReqRepT[Int, Int](2, aThrow)))
    assert(RetryableFailure == classifier(ReqRep(2, aThrow)))
    assert(NonRetryableFailure == classifier(ReqRepT[Int, Int](1, aThrow)))
    assert(NonRetryableFailure == classifier(ReqRep(1, aThrow)))

    assert(!classifier.isDefinedAt(ReqRepT[Int, Int](0, aReturn)))
    assert(!classifier.isDefinedAt(ReqRep(0, aReturn)))
    assert(
      Success == classifier.applyOrElse(ReqRepT[Int, Int](0, aReturn), ResponseClassifier.Default))
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
        rot(reqRepFromException(Failure(timeoutExc, FailureFlags.Interrupted)))
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
