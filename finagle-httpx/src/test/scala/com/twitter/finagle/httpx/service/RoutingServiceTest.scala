package com.twitter.finagle.httpx.service

import com.twitter.finagle.httpx.{Request, Status}
import com.twitter.finagle.httpx.Method.{Get, Post}
import com.twitter.finagle.httpx.path._
import com.twitter.finagle.httpx.path.{Path => FPath}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RoutingServiceTest extends FunSuite {

  test("RoutingService.byPath") {
    val service = RoutingService.byPath {
      case "/test.json" => NullService
    }

    assert(Await.result(service(Request("/test.json"))).status === Status.Ok)
    assert(Await.result(service(Request("/unknown"))).status === Status.NotFound)
  }

  test("RoutingService.byPathObject") {
    val service = RoutingService.byPathObject {
      case Root / "test" ~ "json" => NullService
    }

    assert(Await.result(service(Request("/test.json"))).status === Status.Ok)
    assert(Await.result(service(Request("/unknown"))).status   === Status.NotFound)
  }

  test("RoutingService.byMethodAndPath") {
    val service = RoutingService.byMethodAndPath {
      case (Get, "/test.json") => NullService
    }

    assert(Await.result(service(Request("/test.json"))).status === Status.Ok)
    assert(Await.result(service(Request(Post, "/test.json"))).status === Status.NotFound)
  }

  test("RoutingService.byMethodAndPathObject") {
    val service = RoutingService.byMethodAndPathObject {
      case Get -> Root / "test.json" => NullService
    }

    assert(Await.result(service(Request("/test.json"))).status === Status.Ok)
    assert(Await.result(service(Request(Post, "/test.json"))).status === Status.NotFound)
  }
}
