package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MethodRequiredFilterTest extends FunSuite {

  val dummyService = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = request.response
      request.params.get("exception").foreach(e => {
        response.write("exception thrown")
        throw new Exception()
      })
      request.params.get("code") match {
        case Some(code) => response.statusCode = code.toInt
        case None       => response.status     = Status.Ok
      }
      Future.value(response)
    }
  }

  val filter = new MethodRequiredFilter[Request](Set(Method.Post))

  test("return 407 when disallowed method is used") {
    val request = Request()
    request.method = Method.Get
    val response = Await.result(filter(request, dummyService))
    assert(response.status == Status.MethodNotAllowed)
    assert(response.headers.get("Allow") == "POST")
  }

  test("return 200 when allowed method is used") {
    val request = Request()
    request.method = Method.Post
    val response = Await.result(filter(request, dummyService))
    assert(response.status == Status.Ok)
  }
}
