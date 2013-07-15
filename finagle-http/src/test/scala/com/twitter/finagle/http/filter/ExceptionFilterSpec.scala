package com.twitter.finagle.http.filter

import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.{CancelledRequestException, Service}
import com.twitter.util.{Await, Future}
import org.specs.SpecificationWithJUnit


class ExceptionFilterSpec extends SpecificationWithJUnit {

  val service = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      request.response.write("hello")
      if (request.params.get("exception").isDefined)
        throw new Exception
      else if (request.params.get("throw").isDefined)
        Future.exception(new Exception)
      else if (request.params.get("cancel").isDefined)
        Future.exception(new CancelledRequestException)
      else
        Future.value(request.response)
    }
  }

  "ExceptionFilterSpec" should {
    "ignore success" in {
      val request = Request()
      val filter = (new ExceptionFilter) andThen service

      val response = Await.result(filter(request))
      response.status        must_== Status.Ok
      response.contentString must_== "hello"
    }

    "handle exception" in {
      val request = Request("exception" -> "true")
      val filter = (new ExceptionFilter) andThen service

      val response = Await.result(filter(request))
      response.status        must_== Status.InternalServerError
      response.contentString must_== ""
    }

    "handle throw" in {
      val request = Request("throw" -> "true")
      val filter = (new ExceptionFilter) andThen service

      val response = Await.result(filter(request))
      response.status        must_== Status.InternalServerError
      response.contentString must_== ""
    }

    "handle cancel" in {
      val request = Request("cancel" -> "true")
      val filter = (new ExceptionFilter) andThen service

      val response = Await.result(filter(request))
      response.statusCode    must_== 499
      response.contentString must_== ""
    }
  }
}
