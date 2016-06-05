package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.{Failure, Service}
import com.twitter.util.{Await, Promise, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PendingRequestFilterTest extends FunSuite {

  test("it rejects excessive requests with restartable failures") {
    val svc = Service.mk { p: Future[Unit] =>
      p
    }

    val (p1, p2, p3) = (new Promise[Unit], new Promise[Unit], new Promise[Unit])
    val filteredSvc = new PendingRequestFilter(3).andThen(svc)

    val (r1, r2, r3) = (filteredSvc(p1), filteredSvc(p2), filteredSvc(p3))
    assert(!r1.isDefined)
    assert(!r2.isDefined)
    assert(!r3.isDefined)

    val rejected = intercept[Failure] {
      Await.result(filteredSvc(Future.Done), 3.seconds)
    }

    assert(rejected.isFlagged(Failure.Restartable))

    // one pending request is satisfied
    p1.setDone()
    assert(r1.isDefined)
    assert(!r2.isDefined)
    assert(!r3.isDefined)

    // and a subsequent request is permitted
    val r4 = filteredSvc(Future.Done)
    assert(r4.isDefined)
  }
}
