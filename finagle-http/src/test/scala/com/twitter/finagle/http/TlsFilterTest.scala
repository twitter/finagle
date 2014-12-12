package com.twitter.finagle.http

import com.twitter.util.{Await, Promise, Future}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TlsFilterTest extends FunSuite {
  import HttpVersion._
  import HttpMethod._

  def svc(p: Promise[HttpRequest]) = Service.mk { (req: HttpRequest) =>
    p.setValue(req)
    Future.never
  }

  test("filter") {
    val host = "test.host"
    val tls = new TlsFilter(host)
    val req = new DefaultHttpRequest(HTTP_1_1, GET, "/")
    val p = new Promise[HttpRequest]
    (tls andThen svc(p))(req)
    assert(HttpHeaders.getHost(Await.result(p)) === host)
  }

  test("module") {
    val host = "test.host"
    val p = new Promise[HttpRequest]
    val stk = TlsFilter.module.toStack(
      Stack.Leaf(TlsFilter.role, ServiceFactory.const(svc(p))))
    val fac = stk.make(Stack.Params.empty + Transporter.TLSHostname(Some(host)))
    Await.result(fac())(new DefaultHttpRequest(HTTP_1_1, GET, "/"))
    assert(HttpHeaders.getHost(Await.result(p)) === host)
  }
}
