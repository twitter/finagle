package com.twitter.finagle.http.filter

import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.{CancelledRequestException, Service}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExceptionFilterTest extends FunSuite {

  val service = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      request.response.write("hello")
      request.response.contentLength = 5
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

  test("ignore success") {
    val request = Request()
    val filter = (new ExceptionFilter) andThen service

    val response = Await.result(filter(request))
    assert(response.status        == Status.Ok)
    assert(response.contentString ==  "hello")
    assert(response.contentLength == Some(5))
  }

  test("handle exception") {
    val request = Request("exception" -> "true")
    val filter = (new ExceptionFilter) andThen service

    val response = Await.result(filter(request))
    assert(response.status        == Status.InternalServerError)
    assert(response.contentString == "")
    assert(response.contentLength == Some(0))
  }

  test("handle throw") {
    val request = Request("throw" -> "true")
    val filter = (new ExceptionFilter) andThen service

    val response = Await.result(filter(request))
    assert(response.status        == Status.InternalServerError)
    assert(response.contentString == "")
    assert(response.contentLength == Some(0))
  }

  test("handle cancel") {
    val request = Request("cancel" -> "true")
    val filter = (new ExceptionFilter) andThen service

    val response = Await.result(filter(request))
    assert(response.statusCode    == 499)
    assert(response.contentString == "")
    assert(response.contentLength == Some(0))
  }
}
