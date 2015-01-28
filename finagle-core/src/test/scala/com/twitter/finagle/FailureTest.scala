package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.junit.{JUnitRunner, AssertionsForJUnit}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class FailureTest extends FunSuite with AssertionsForJUnit with GeneratorDrivenPropertyChecks {
  val exc = Gen.oneOf(
    new Exception("first"),
    new Exception("second"))

  val failure = Gen.oneOf[Throwable => Failure](
    Failure.Cause(_: Throwable), 
    Failure.InterruptedBy(_: Throwable), 
    Failure.Retryable(_: Throwable), 
    Failure.Rejected(_: Throwable))

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
  
  test("rejected failures") {
    val exc = new Exception
    val f = Failure.Rejected(exc)
    
    f match {
      case Failure.Rejected(e) => assert(e == exc)
      case _ => fail()
    }
    
    // Rejected is a subtype of retryable.
    f match {
      case Failure.Retryable(e) => assert(e == exc)
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
  }
  
  test("equality (gen)") {
    forAll(failure) { f =>
      val e1, e2 = new Exception
      f(e1) == f(e1) && f(e1) != f(e2) && f(e1).hashCode == f(e1).hashCode
    }
    
    val failure2 = for (f1 <- failure; f2 <- failure if f1 != f2) yield (f1, f2)
    forAll(failure2) { case (f1, f2) =>
      val e = new Exception
      f1(e) != f2(e)
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
