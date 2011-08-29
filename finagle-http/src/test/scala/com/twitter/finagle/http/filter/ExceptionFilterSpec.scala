package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.Future
import org.specs.Specification


object ExceptionFilterSpec extends Specification {

  val service = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      request.response.write("hello")
      if (request.params.get("exception").isDefined)
        throw new Exception
      else if (request.params.get("throw").isDefined)
        Future.exception(new Exception)
      else
        Future.value(request.response)
    }
  }

  "ExceptionFilterSpec" should {
    "ignore success" in {
      val request = Request()
      val filter = (new ExceptionFilter) andThen service

      val response = filter(request)()
      response.status        must_== Status.Ok
      response.contentString must_== "hello"
    }

    "handle exception" in {
      val request = Request("exception" -> "true")
      val filter = (new ExceptionFilter) andThen service

      val response = filter(request)()
      response.status        must_== Status.InternalServerError
      response.contentString must_== ""
    }

    "handle throw" in {
      val request = Request("throw" -> "true")
      val filter = (new ExceptionFilter) andThen service

      val response = filter(request)()
      response.status        must_== Status.InternalServerError
      response.contentString must_== ""
    }
  }
}
