package com.twitter.finagle.http.service

import org.specs.Specification
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.path.{Path => FPath}


object RoutingServiceSpec extends Specification {

  "RoutingServiceSpec" should {
    "RoutingService.byPath" in {
      val service = RoutingService.byPath {
        case "/test.json" => NullService
      }

      service(Request("/test.json"))().status must_== Status.Ok
      service(Request("/unknown"))().status   must_== Status.NotFound
    }

    "RoutingService.byPathObject" in {
      val service = RoutingService.byPathObject {
        case Root / "test" ~ "json" => NullService
      }

      service(Request("/test.json"))().status must_== Status.Ok
      service(Request("/unknown"))().status   must_== Status.NotFound
    }
  }
}
