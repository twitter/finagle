package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response, Status, Method}
import com.twitter.util.{Await, Future, Duration}
import org.scalatest.funsuite.AnyFunSuite

class CorsTest extends AnyFunSuite {
  val TRAP = Method("TRAP")
  val underlying = Service.mk[Request, Response] { request =>
    val response = Response()
    if (request.method == TRAP) {
      response.contentString = "#guwop"
    } else {
      response.status = Status.MethodNotAllowed
    }
    Future.value(response)
  }

  val policy = Cors.Policy(
    allowsOrigin = {
      case origin if origin.startsWith("juug") => Some(origin)
      case origin if origin.endsWith("street") => Some(origin)
      case _ => None
    },
    allowsMethods = method => Some(Seq(method, "TRAP")),
    allowsHeaders = headers => Some(headers),
    exposedHeaders = Seq("Icey"),
    supportsCredentials = true,
    maxAge = Some(Duration.Top)
  )

  val corsFilter = new Cors.HttpFilter(policy)
  val service = corsFilter.andThen(underlying)

  private[this] def assertHeader(response: Response, name: String, expected: String): Unit =
    response.headerMap.get(name) match {
      case Some(actual) => assert(expected == actual)
      case None => fail(s"$name not found")
    }

  private[this] def assertHeaderMissing(response: Response, name: String): Unit =
    assert(response.headerMap.get(name).isEmpty)

  test("Cors.HttpFilter handles preflight requests") {
    val request = Request()
    request.method = Method.Options
    request.headerMap.set("Origin", "thestreet")
    request.headerMap.set("Access-Control-Request-Method", "BRR")

    val response = Await.result(service(request), 1.second)
    assertHeader(response, "Access-Control-Allow-Origin", "thestreet")
    assertHeader(response, "Access-Control-Allow-Credentials", "true")
    assertHeader(response, "Access-Control-Allow-Methods", "BRR, TRAP")
    assertHeader(response, "Vary", "Origin")
    assertHeader(response, "Access-Control-Max-Age", Duration.Top.inSeconds.toString)
    assert(response.contentString == "")
  }

  test("Http.CorsFilter responds to invalid preflight requests without CORS headers") {
    val request = Request()
    request.method = Method.Options

    val response = Await.result(service(request), 1.second)
    assert(response.status == Status.Ok)
    assertHeaderMissing(response, "Access-Control-Allow-Origin")
    assertHeaderMissing(response, "Access-Control-Allow-Credentials")
    assertHeaderMissing(response, "Access-Control-Allow-Methods")
    assertHeader(response, "Vary", "Origin")
    assert(response.contentString == "")
  }

  test("Http.CorsFilter responds to unacceptable cross-origin requests without CORS headers") {
    val request = Request()
    request.method = Method.Options
    request.headerMap.set("Origin", "theclub")

    val response = Await.result(service(request), 1.second)
    assert(response.status == Status.Ok)
    assertHeaderMissing(response, "Access-Control-Allow-Origin")
    assertHeaderMissing(response, "Access-Control-Allow-Credentials")
    assertHeaderMissing(response, "Access-Control-Allow-Methods")
    assertHeader(response, "Vary", "Origin")
    assert(response.contentString == "")
  }

  test("Http.CorsFilter handles simple requests") {
    val request = Request()
    request.method = TRAP
    request.headerMap.set("Origin", "juughaus")

    val response = Await.result(service(request), 1.second)
    assertHeader(response, "Access-Control-Allow-Origin", "juughaus")
    assertHeader(response, "Access-Control-Allow-Credentials", "true")
    assertHeader(response, "Access-Control-Expose-Headers", "Icey")
    assertHeader(response, "Vary", "Origin")
    assert(response.contentString == "#guwop")
  }

  test(
    "Http.CorsFilter does not add response headers to simple requests if request headers aren't present") {
    val request = Request()
    request.method = TRAP

    val response = Await.result(service(request), 1.second)
    assertHeaderMissing(response, "Access-Control-Allow-Origin")
    assertHeaderMissing(response, "Access-Control-Allow-Credentials")
    assertHeaderMissing(response, "Access-Control-Expose-Headers")
    assertHeader(response, "Vary", "Origin")
    assert(response.contentString == "#guwop")
  }
}
