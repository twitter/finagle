package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.{CancelledRequestException, Service}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class ExceptionFilterTest extends AnyFunSuite {

  val service = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = Response()
      response.write("hello")
      response.contentLength = 5
      if (request.params.get("exception").isDefined)
        throw new Exception
      else if (request.params.get("throw").isDefined)
        Future.exception(new Exception)
      else if (request.params.get("cancel").isDefined)
        Future.exception(new CancelledRequestException)
      else
        Future.value(response)
    }
  }

  val filteredService = (new ExceptionFilter).andThen(service)

  test("ignore success") {
    val request = Request()
    val response = Await.result(filteredService(request), 1.second)

    assert(response.status == Status.Ok)
    assert(response.contentString == "hello")
    assert(response.contentLength == Some(5))
  }

  test("handle exception") {
    val request = Request("exception" -> "true")
    val response = Await.result(filteredService(request), 1.second)

    assert(response.status == Status.InternalServerError)
    assert(response.contentString == "")
    assert(response.contentLength == None)
  }

  test("handle throw") {
    val request = Request("throw" -> "true")
    val response = Await.result(filteredService(request), 1.second)

    assert(response.status == Status.InternalServerError)
    assert(response.contentString == "")
    assert(response.contentLength == None)
  }

  test("handle cancel") {
    val request = Request("cancel" -> "true")
    val response = Await.result(filteredService(request), 1.second)

    assert(response.statusCode == 499)
    assert(response.contentString == "")
    assert(response.contentLength == None)
  }
}
