package com.twitter.finagle.http.filter

import com.twitter.finagle.http.codec.HttpDtab
import com.twitter.finagle.http.{Status, Response, Request}
import com.twitter.finagle.{Service, Dtab}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class DtabFilterTest extends FunSuite with AssertionsForJUnit {

  test("parses X-Dtab headers") {
    val dtab = Dtab.read("/foo=>/bar;/bah=>/baz")
    val service =
      new DtabFilter.Finagle[Request] andThen
      Service.mk[Request, Response] { req =>
        val xDtabHeaders = req.headerMap.keys.filter(_.toLowerCase startsWith "x-dtab")
        assert(xDtabHeaders.isEmpty, "x-dtab headers not cleared")
        assert(Dtab.local == dtab)
        Future.value(Response())
      }

    val req = Request()
    HttpDtab.write(dtab, req)
    Dtab.unwind {
      Dtab.local = Dtab.empty
      val rsp = Await.result(service(req))
      assert(rsp.status == Status.Ok)
    }
  }

  test("responds with an error on invalid dtab headers") {
    val service =
      new DtabFilter.Finagle[Request] andThen
      Service.mk[Request, Response] { _ =>
        fail("request should not have reached service")
        Future.value(Response())
      }

    val req = Request()
    // not base64 encoded
    req.headers().add("X-Dtab-01-A", "/foo")
    req.headers().add("X-Dtab-01-B", "/bar")
    Dtab.unwind {
      Dtab.local = Dtab.empty
      val rsp = Await.result(service(req))
      assert(rsp.status == Status.BadRequest)
      assert(rsp.contentType == Some("text/plain; charset=UTF-8"))
      assert(rsp.contentLength.getOrElse(0L) > 0, "content-length is zero")
      assert(rsp.contentString.nonEmpty, "content is empty")
    }
  }
}
