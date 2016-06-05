package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{MediaType, Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonpFilterTest extends FunSuite {

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

  test("wrap json") {
    val request = Request("/test.json", "callback" -> "mycallback")
    val response = Await.result(JsonpFilter(request, dummyService))

    assert(response.contentType   == Some("application/javascript"))
    assert(response.contentString == "/**/mycallback({});")
  }

  test("ignore non-json") {
    val request = Request("/test.json", "callback" -> "mycallback", "not_json" -> "t")
    val response = Await.result(JsonpFilter(request, dummyService))

    assert(response.mediaType     == Some("not_json"))
    assert(response.contentString == "{}")
    assert(response.contentType   == Some("not_json"))
  }

  test("ignore HEAD") {
    val request = Request("/test.json", "callback" -> "mycallback")
    request.method = Method.Head

    val response = Await.result(JsonpFilter(request, dummyService))
    assert(response.contentType   == Some("application/json"))
    assert(response.contentString == "{}")
  }

  test("ignore empty callback") {
    // Search Varnish sets callback to blank.  These should not be wrapped.
    val request = Request("/test.json", "callback" -> "")

    val response = Await.result(JsonpFilter(request, dummyService))
    assert(response.contentType   == Some("application/json"))
    assert(response.contentString == "{}")
  }
}
