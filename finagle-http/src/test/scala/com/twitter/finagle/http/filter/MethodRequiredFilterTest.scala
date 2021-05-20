package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class MethodRequiredFilterTest extends AnyFunSuite {

  val dummyService = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = Response()
      request.params
        .get("exception")
        .foreach(e => {
          response.write("exception thrown")
          throw new Exception()
        })
      request.params.get("code") match {
        case Some(code) => response.statusCode = code.toInt
        case None => response.status = Status.Ok
      }
      Future.value(response)
    }
  }

  val filter = new MethodRequiredFilter[Request](Set(Method.Post))

  test("return 407 when disallowed method is used") {
    val request = Request()
    request.method = Method.Get
    val response = Await.result(filter(request, dummyService), 1.second)
    assert(response.status == Status.MethodNotAllowed)
    assert(response.headerMap.get("Allow") == Some("POST"))
  }

  test("return 200 when allowed method is used") {
    val request = Request()
    request.method = Method.Post
    val response = Await.result(filter(request, dummyService), 1.second)
    assert(response.status == Status.Ok)
  }
}
