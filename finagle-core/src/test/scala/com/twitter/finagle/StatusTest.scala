package com.twitter.finagle

import com.twitter.util.{Future, Promise}
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class StatusTest extends FunSuite with AssertionsForJUnit with GeneratorDrivenPropertyChecks {
  val foreverBusy = Status.Busy(Future.never)

  val status1 = Gen.oneOf(Status.Open, foreverBusy, Status.Closed)
  val status2 = for (left <- status1; right <- status1) yield (left, right)

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

  test("Status.worst(Busy, Busy)") {
    val p1, p2 = new Promise[Unit]
    val Status.Busy(p3) = Status.worst(Status.Busy(p1), Status.Busy(p2))
    assert(!p3.isDefined)
    p1.setDone()
    assert(!p3.isDefined)
    p2.setDone()
    assert(p3.isDefined)
  }

  test("Status.best(Busy, Busy)") {
    val p1, p2 = new Promise[Unit]
    val Status.Busy(p3) = Status.best(Status.Busy(p1), Status.Busy(p2))
    assert(!p3.isDefined)
    p1.setDone()
    assert(p3.isDefined)
  }
  
  test("Ordering spot check") {
    val ord = Array(Status.Closed, foreverBusy, Status.Open)
    val idx2 = for { left <- Gen.choose(0, ord.length-1); 
      right <- Gen.choose(0, ord.length-1) } yield (left, right)

    forAll(idx2) { case (left, right) =>
      Ordering[Status].compare(ord(left), ord(right)).signum == (left - right).signum
    }
  }
}
