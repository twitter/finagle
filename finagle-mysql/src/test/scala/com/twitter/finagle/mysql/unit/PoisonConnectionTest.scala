package com.twitter.finagle.mysql.unit

import com.twitter.finagle.mysql.PoisonConnection.PoisonedConnectionException
import com.twitter.finagle.{Service, Status}
import com.twitter.finagle.mysql.{CloseRequest, PoisonConnectionRequest, Request, Result}
import com.twitter.util.{Future, Promise, Throw, Time}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class PoisonableServiceFactoryTest extends FunSuite with MockitoSugar {

  import com.twitter.finagle.mysql.PoisonConnection.PoisonableService

  private def mockService = {
    val svc = mock[Service[Request, Result]]
    when(svc.close(any[Time])).thenReturn(Future.Done)
    when(svc.status).thenReturn(Status.Open)
    svc
  }

  test("PoisonableService can be poisoned") {
    val underlying = mockService
    val svc = new PoisonableService(underlying)
    assert(svc.status == Status.Open)

    val p = Promise[Result]()
    // Send a request
    when(underlying.apply(any[Request])).thenReturn(p)
    assert(svc(CloseRequest(2)) eq p)

    svc(PoisonConnectionRequest).poll match {
      case Some(Throw(_: PoisonedConnectionException)) => // ok
      case other => fail(s"Unexpected result: $other")
    }

    assert(svc.status == Status.Closed)
    svc(CloseRequest(2)).poll match {
      case Some(Throw(_: PoisonedConnectionException)) => // ok
      case other => fail(s"Unexpected result: $other")
    }
  }
}
