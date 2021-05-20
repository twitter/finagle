package com.twitter.finagle.http

import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.util.{Await, Promise, Future}
import org.scalatest.funsuite.AnyFunSuite

class TlsFilterTest extends AnyFunSuite {
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
    val stk = TlsFilter.module.toStack(Stack.leaf(TlsFilter.role, ServiceFactory.const(svc(p))))
    val fac = stk.make(
      Stack.Params.empty + Transport.ClientSsl(Some(SslClientConfiguration(hostname = Some(host))))
    )
    Await.result(fac())(Request(Http11, Get, "/"))
    assert(Await.result(p).headerMap.get("Host") == Some(host))
  }
}
