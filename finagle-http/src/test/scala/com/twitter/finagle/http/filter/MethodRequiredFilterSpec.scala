package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.specs.SpecificationWithJUnit

class MethodRequiredFilterSpec extends SpecificationWithJUnit {

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

  "disable filter" should {
    "return 407 when disallowed method is used" in {
      val request = Request()
      request.method = Method.Get
      val response = Await.result(filter(request, dummyService))
      response.status must_== Status.MethodNotAllowed
      response.headers.get("Allow").get must be_==("POST")
    }

    "return 200 when allowed method is used" in {
      val request = Request()
      request.method = Method.Post
      val response = Await.result(filter(request, dummyService))
      response.status must_== Status.Ok
    }
  }
}
