package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.codec.HttpDtab
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.{Dtab, Service}
import com.twitter.util.{Await, Future}
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatest.funsuite.AnyFunSuite

class DtabFilterTest extends AnyFunSuite with AssertionsForJUnit {

  private val timeout = 2.seconds

  test("Extractor parses X-Dtab headers") {
    val dtab = Dtab.read("/foo=>/bar;/bah=>/baz")
    val service = new DtabFilter.Extractor().andThen(Service.mk[Request, Response] { req =>
      val xDtabHeaders = req.headerMap.keys.filter(_.toLowerCase startsWith "x-dtab")
      assert(xDtabHeaders.isEmpty, "x-dtab headers not cleared")
      assert(Dtab.local == dtab)
      Future.value(Response())
    })

    val req = Request()
    HttpDtab.write(dtab, req)
    Dtab.unwind {
      Dtab.local = Dtab.empty
      val rsp = Await.result(service(req), timeout)
      assert(rsp.status == Status.Ok)
    }
  }

  test("Extractor responds with an error on invalid dtab headers") {
    val service = new DtabFilter.Extractor().andThen(Service.mk[Request, Response] { _ =>
      fail("request should not have reached service")
      Future.value(Response())
    })

    val req = Request()
    // not base64 encoded
    req.headerMap.add("X-Dtab-01-A", "/foo")
    req.headerMap.add("X-Dtab-01-B", "/bar")
    Dtab.unwind {
      Dtab.local = Dtab.empty
      val rsp = Await.result(service(req), timeout)
      assert(rsp.status == Status.BadRequest)
      assert(rsp.contentType == Some("text/plain; charset=UTF-8"))
      assert(rsp.contentLength.getOrElse(0L) > 0, "content-length is zero")
      assert(rsp.contentString.nonEmpty, "content is empty")
    }
  }

  test("Injector strips new-style dtabs") {
    @volatile var receivedDtab: Option[Dtab] = None
    val svc = new DtabFilter.Injector().andThen(Service.mk[Request, Response] { req =>
      receivedDtab = HttpDtab.read(req).toOption
      Future.value(Response())
    })

    // prepare a request and add a correct looking new-style dtab header
    val req = Request()
    req.headerMap.add("dtab-local", "/srv=>/srv#/staging")
    Await.result(svc(req), timeout)

    assert(receivedDtab == Some(Dtab.empty))
  }

  test("Injector strips old-style dtabs") {
    @volatile var receivedDtab: Option[Dtab] = None
    val svc = new DtabFilter.Injector().andThen(Service.mk[Request, Response] { req =>
      receivedDtab = HttpDtab.read(req).toOption
      Future.value(Response())
    })

    // prepare a request and add a correct looking new-style dtab header
    val req = Request()
    req.headerMap.add("x-dtab-00-a", "/s/foo")
    req.headerMap.add("x-dtab-00-b", "/s/bar")
    Await.result(svc(req), timeout)

    assert(receivedDtab == Some(Dtab.empty))
  }

  test("Injector transmits dtabs") {
    @volatile var receivedDtab: Option[Dtab] = None
    val svc = new DtabFilter.Injector().andThen(Service.mk[Request, Response] { req =>
      receivedDtab = HttpDtab.read(req).toOption
      Future.value(Response())
    })

    Dtab.unwind {
      val origDtab = Dtab.read("/s => /srv/smf1")
      Dtab.local = origDtab

      // prepare a request and add a correct looking new-style dtab header
      val req = Request()
      Await.result(svc(req), timeout)

      assert(receivedDtab == Some(origDtab))
    }
  }

  test("Injector does not transmit dtab.limited") {
    var receivedDtab: Option[Dtab] = None
    val svc = new DtabFilter.Injector().andThen(Service.mk[Request, Response] { req =>
      receivedDtab = HttpDtab.read(req).toOption
      Future.value(Response())
    })

    Dtab.unwind {
      val newDtab = Dtab.read("/s => /srv/smf1")
      Dtab.limited = newDtab

      // prepare a request and add a correct looking new-style dtab header
      val req = Request()
      Await.result(svc(req), timeout)

      assert(receivedDtab == Some(Dtab.empty))
    }
  }
}
