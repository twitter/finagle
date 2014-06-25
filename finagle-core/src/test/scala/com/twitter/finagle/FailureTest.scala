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

  test("equality") {
    val ex_a = new Exception("fonzbonz")
    val ex_b = new Exception("otherone")
    val fa1: Failure = Failure.Retryable(ex_a)
    val fa2: Failure = Failure.Retryable(ex_a)
    val fb: Failure = Failure.Retryable(ex_b)
    val faa: Failure = Failure.InterruptedBy(ex_a)

    assert(fa1 === fa2)
    assert(fa1.hashCode === fa2.hashCode)
    assert(fa1 != fb)
    assert(fa2 != faa)
    assert(fa2.hashCode != fb.hashCode)
    assert(fa1.hashCode != faa.hashCode)
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
