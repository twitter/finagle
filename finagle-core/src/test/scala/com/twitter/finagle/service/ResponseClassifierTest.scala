package com.twitter.finagle.service

import com.twitter.finagle.{Failure, WriteException}
import com.twitter.finagle.service.ResponseClass._
import com.twitter.util.{Return, Throw}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResponseClassifierTest extends FunSuite {

  test("Default classification") {
    assert("DefaultResponseClassifier" == ResponseClassifier.Default.toString)
    assert(Success ==
      ResponseClassifier.Default(ReqRep(null, Return("hi"))))

    assert(RetryableFailure ==
      ResponseClassifier.Default(ReqRep(null, Throw(Failure.rejected))))

    assert(NonRetryableFailure ==
      ResponseClassifier.Default(ReqRep(null, Throw(Failure("nope")))))
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

}
