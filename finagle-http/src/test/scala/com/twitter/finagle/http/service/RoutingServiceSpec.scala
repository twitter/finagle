package com.twitter.finagle.http.service

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.http.{Request, Status}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.path.{Path => FPath}
import com.twitter.util.Await


class RoutingServiceSpec extends SpecificationWithJUnit {

  "RoutingServiceSpec" should {
    "RoutingService.byPath" in {
      val service = RoutingService.byPath {
        case "/test.json" => NullService
      }

      Await.result(service(Request("/test.json"))).status must_== Status.Ok
      Await.result(service(Request("/unknown"))).status   must_== Status.NotFound
    }

    "RoutingService.byPathObject" in {
      val service = RoutingService.byPathObject {
        case Root / "test" ~ "json" => NullService
      }

      Await.result(service(Request("/test.json"))).status must_== Status.Ok
      Await.result(service(Request("/unknown"))).status   must_== Status.NotFound
    }
  }
}
