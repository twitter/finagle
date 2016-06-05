package com.twitter.finagle

import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class StatusTest
  extends FunSuite
  with AssertionsForJUnit
  with GeneratorDrivenPropertyChecks
  with Eventually
  with IntegrationPatience {

  val status1 = Gen.oneOf(Status.Open, Status.Busy, Status.Closed)
  val status2 = for (left <- status1; right <- status1) yield (left, right)

  test("Status.bestOf can terminate early") {
    val res = Status.bestOf[Function0[Status]](
      List(() => Status.Busy, () => Status.Open, () => fail("element should not be evaluated")),
      _.apply
    )
    assert(res == Status.Open)
  }

  test("Status.worstOf can terminate early") {
    val res = Status.worstOf[Function0[Status]](
      List(() => Status.Busy, () => Status.Closed, () => fail("element should not be evaluated")),
      _.apply
    )
    assert(res == Status.Closed)
  }

  // This test is borderline silly.
  test("Status.worst") {
    forAll(status2) { case (left, right) =>
      val s = Status.worst(left, right)
      Ordering[Status].equiv(left, right) || s == Ordering[Status].max(left, right)
    }
  }

  // This test is borderline silly.
  test("Status.best") {
    forAll(status2) { case (left, right) =>
      val s = Status.best(left, right)
      Ordering[Status].equiv(left, right) || s == Ordering[Status].min(left, right)
    }
  }

  test("Status.whenOpen - opens") {
    @volatile var status: Status = Status.Busy
    val open = Status.whenOpen(status)

    assert(!open.isDone)

    status = Status.Open
    eventually { assert(open.isDone) }
    Await.result(open)  // no exceptions
  }

  test("Status.whenOpen - closes") {
    @volatile var status: Status = Status.Busy
    val open = Status.whenOpen(status)

    assert(!open.isDone)

    status = Status.Closed
    eventually { assert(open.isDefined) }
    intercept[Status.ClosedException] { Await.result(open) }
  }

  test("Ordering spot check") {
    val ord = Array(Status.Closed, Status.Busy, Status.Open)
    val idx2 = for { left <- Gen.choose(0, ord.length-1);
      right <- Gen.choose(0, ord.length-1) } yield (left, right)

    forAll(idx2) { case (left, right) =>
      Ordering[Status].compare(ord(left), ord(right)).signum == (left - right).signum
    }
  }
}
