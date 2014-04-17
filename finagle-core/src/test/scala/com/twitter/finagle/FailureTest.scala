package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FailureTest extends FunSuite {
  test("simple failures with a cause") {
    val why = "boom!"
    val exc = new Exception(why)
    val failure = Failure.Cause(exc)
    failure match {
      case Failure.Retryable(_) => fail()
      case Failure.InterruptedBy(_) => fail()
      case f@Failure.Cause(e) =>
        assert(f.why === why)
        assert(e === exc)
      case _ => fail()
    }
  }

  test("retryable failures") {
    val exc = new Exception
    val f = Failure.Retryable(exc)
    f match {
      case Failure.Retryable(e) => assert(e === exc)
      case _ => fail()
    }

    f.withRetryable(false) match {
      case Failure.Retryable(e) => fail()
      case _ => // pass
    }

    f.withRetryable(true) match {
      case Failure.Retryable(_) => // pass
      case _ => fail()
    }
  }

  test("interrupted failures") {
    val exc = new Exception
    val f = Failure.InterruptedBy(exc)
    f match {
      case Failure.InterruptedBy(e) => assert(e === exc)
      case _ => fail()
    }
  }
}