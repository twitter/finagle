package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.specs.SpecificationWithJUnit

class HeadFilterSpec extends SpecificationWithJUnit {
  val Body = "hello world"

  val dummyService = new Service[Request, Response] {
   def apply(request: Request) = {
     request.method must be_==(Method.Get)
     val response = request.response
     response.status = Status.Ok
     response.write(Body)
     Future.value(response)
   }
  }

  "HeadFilter" should {
    "convert GET to HEAD" in {
      val request = Request("/test.json")
      request.method = Method.Head

      val response = Await.result(HeadFilter(request, dummyService))
      request.method must be_==(Method.Head) // unchanged
      response.contentLength must be_==(Some(Body.length))
      response.contentString must be_==("")
    }

    "GET is normal" in {
      val request = Request("/test.json")

      val response = Await.result(HeadFilter(request, dummyService))
      request.method must be_==(Method.Get) // unchanged
      response.contentLength must be_==(None)
      response.contentString must be_==(Body)
    }
  }
}
