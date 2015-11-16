package com.twitter.finagle.http

import com.twitter.util.{Await, Promise, Future}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TlsFilterTest extends FunSuite {
  import Version._
  import Method._

  def svc(p: Promise[Request]) = Service.mk { (req: Request) =>
    p.setValue(req)
    Future.never
  }

  test("filter") {
    val host = "test.host"
    val tls = new TlsFilter(host)
    val req = Request(Http11, Get, "/")
    val p = new Promise[Request]
    (tls andThen svc(p))(req)
    assert(Await.result(p).headerMap.get("Host") == Some(host))
  }

  test("module") {
    val host = "test.host"
    val p = new Promise[Request]
    val stk = TlsFilter.module.toStack(
      Stack.Leaf(TlsFilter.role, ServiceFactory.const(svc(p))))
    val fac = stk.make(Stack.Params.empty + Transporter.TLSHostname(Some(host)))
    Await.result(fac())(Request(Http11, Get, "/"))
    assert(Await.result(p).headerMap.get("Host") == Some(host))
  }
}
