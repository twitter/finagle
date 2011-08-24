package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.Future
import org.specs.Specification

object JsonpFilterSpec extends Specification {

  val dummyService = new Service[Request, Response] {
   def apply(request: Request): Future[Response] = {
     val response = request.response
     response.status = Status.Ok
     if (request.params.contains("not_json"))
       response.mediaType = "not_json"
     else
       response.setContentTypeJson()
     response.write("{}")
     Future.value(response)
   }
  }

  "JsonpFilter" should {
    "wrap json" in {
      val request = Request("/test.json", "callback" -> "mycallback")

      val response = JsonpFilter(request, dummyService)()
      response.contentType   must_== Some("application/json;charset=utf-8")
      response.contentString must_== "mycallback({});"
    }

    "ignore non-json" in {
      val request = Request("/test.json", "callback" -> "mycallback", "not_json" -> "t")

      val response = JsonpFilter(request, dummyService)()
      response.mediaType     must_== Some("not_json")
      response.contentString must_== "{}"
    }

    "ignore HEAD" in {
      val request = Request("/test.json", "callback" -> "mycallback")
      request.method = Method.Head

      val response = JsonpFilter(request, dummyService)()
      response.contentType   must_== Some("application/json;charset=utf-8")
      response.contentString must_== "{}"
    }

    "ignore empty callback" in {
      // Search Varnish sets callback to blank.  These should not be wrapped.
      val request = Request("/test.json", "callback" -> "")

      val response = JsonpFilter(request, dummyService)()
      response.contentType   must_== Some("application/json;charset=utf-8")
      response.contentString must_== "{}"
    }

  }
}
