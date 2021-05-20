package com.twitter.finagle.service

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Service, ServiceClosedException, Status}
import com.twitter.util.{Await, Future, Time}
import org.scalatest.funsuite.AnyFunSuite

class ClosableServiceTest extends AnyFunSuite {
  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  trait Ctx {
    var numClosed = 0
    private val noOpService = new Service[Unit, Unit] {
      def apply(req: Unit): Future[Unit] = Future.Done
      override def close(time: Time) = {
        numClosed += 1
        Future.Done
      }
    }

    val svc = new ClosableService(noOpService) {
      val closedException = new ServiceClosedException
    }
  }

  test("cannot reuse a closed session")(new Ctx {
    await(svc())

    assert(svc.status == Status.Open)
    assert(numClosed == 0)

    await(svc.close())
    intercept[Exception] {
      await(svc())
    }

    assert(svc.status == Status.Closed)
    assert(numClosed == 1)
  })

  test("only closes the underlying session once")(new Ctx {
    await(svc.close())
    await(svc.close())

    assert(numClosed == 1)
  })
}
