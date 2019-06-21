package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response, Status, Method}
import com.twitter.util.{Await, Future, Duration}
import org.scalatest.{FlatSpec, MustMatchers}

class CorsTest extends FlatSpec with MustMatchers {
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
    allowsMethods = (method => Some(Seq(method, "TRAP"))),
    allowsHeaders = (headers => Some(headers)),
    exposedHeaders = Seq("Icey"),
    supportsCredentials = true,
    maxAge = Some(Duration.Top)
  )

  val corsFilter = new Cors.HttpFilter(policy)
  val service = corsFilter.andThen(underlying)

  "Cors.HttpFilter" should "handle preflight requests" in {
    val request = Request()
    request.method = Method.Options
    request.headerMap.set("Origin", "thestreet")
    request.headerMap.set("Access-Control-Request-Method", "BRR")

    val response = Await.result(service(request), 1.second)
    response.headerMap.get("Access-Control-Allow-Origin") must be(Some("thestreet"))
    response.headerMap.get("Access-Control-Allow-Credentials") must be(Some("true"))
    response.headerMap.get("Access-Control-Allow-Methods") must be(Some("BRR, TRAP"))
    response.headerMap.get("Vary") must be(Some("Origin"))
    response.headerMap.get("Access-Control-Max-Age") must be(Some(Duration.Top.inSeconds.toString))
    response.contentString must be("")
  }

  it should "respond to invalid preflight requests without CORS headers" in {
    val request = Request()
    request.method = Method.Options

    val response = Await.result(service(request), 1.second)
    response.status must be(Status.Ok)
    response.headerMap.get("Access-Control-Allow-Origin") must be(None)
    response.headerMap.get("Access-Control-Allow-Credentials") must be(None)
    response.headerMap.get("Access-Control-Allow-Methods") must be(None)
    response.headerMap.get("Vary") must be(Some("Origin"))
    response.contentString must be("")
  }

  it should "respond to unacceptable cross-origin requests without CORS headers" in {
    val request = Request()
    request.method = Method.Options
    request.headerMap.set("Origin", "theclub")

    val response = Await.result(service(request), 1.second)
    response.status must be(Status.Ok)
    response.headerMap.get("Access-Control-Allow-Origin") must be(None)
    response.headerMap.get("Access-Control-Allow-Credentials") must be(None)
    response.headerMap.get("Access-Control-Allow-Methods") must be(None)
    response.headerMap.get("Vary") must be(Some("Origin"))
    response.contentString must be("")
  }

  it should "handle simple requests" in {
    val request = Request()
    request.method = TRAP
    request.headerMap.set("Origin", "juughaus")

    val response = Await.result(service(request), 1.second)
    response.headerMap.get("Access-Control-Allow-Origin") must be(Some("juughaus"))
    response.headerMap.get("Access-Control-Allow-Credentials") must be(Some("true"))
    response.headerMap.get("Access-Control-Expose-Headers") must be(Some("Icey"))
    response.headerMap.get("Vary") must be(Some("Origin"))
    response.contentString must be("#guwop")
  }

  it should "not add response headers to simple requests if request headers aren't present" in {
    val request = Request()
    request.method = TRAP

    val response = Await.result(service(request), 1.second)
    response.headerMap.get("Access-Control-Allow-Origin") must be(None)
    response.headerMap.get("Access-Control-Allow-Credentials") must be(None)
    response.headerMap.get("Access-Control-Expose-Headers") must be(None)
    response.headerMap.get("Vary") must be(Some("Origin"))
    response.contentString must be("#guwop")
  }
}
