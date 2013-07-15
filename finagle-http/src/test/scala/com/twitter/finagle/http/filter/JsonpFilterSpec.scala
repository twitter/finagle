package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{MediaType, Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.specs.SpecificationWithJUnit

class JsonpFilterSpec extends SpecificationWithJUnit {

  val dummyService = new Service[Request, Response] {
   def apply(request: Request): Future[Response] = {
     val response = request.response
     response.status = Status.Ok
     if (request.params.contains("not_json"))
       response.mediaType = "not_json"
     else
       response.mediaType = MediaType.Json
     response.write("{}")
     Future.value(response)
   }
  }

  "JsonpFilter" should {
    "wrap json" in {
      val request = Request("/test.json", "callback" -> "mycallback")

      val response = Await.result(JsonpFilter(request, dummyService))
      response.contentType   must_== Some("application/javascript")
      response.contentString must_== "mycallback({});"
    }

    "ignore non-json" in {
      val request = Request("/test.json", "callback" -> "mycallback", "not_json" -> "t")

      val response = Await.result(JsonpFilter(request, dummyService))
      response.mediaType     must_== Some("not_json")
      response.contentString must_== "{}"
      response.contentType   must_== Some("not_json")
    }

    "ignore HEAD" in {
      val request = Request("/test.json", "callback" -> "mycallback")
      request.method = Method.Head

      val response = Await.result(JsonpFilter(request, dummyService))
      response.contentType   must_== Some("application/json")
      response.contentString must_== "{}"
    }

    "ignore empty callback" in {
      // Search Varnish sets callback to blank.  These should not be wrapped.
      val request = Request("/test.json", "callback" -> "")

      val response = Await.result(JsonpFilter(request, dummyService))
      response.contentType   must_== Some("application/json")
      response.contentString must_== "{}"
    }

  }
}
