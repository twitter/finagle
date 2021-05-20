package com.twitter.finagle.http

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.{Address, Service, ServiceFactory, Stack}
import com.twitter.conversions.DurationOps._
import com.twitter.util.{Await, Future, Promise}
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class LoadBalancedHostFilterTest extends AnyFunSuite {
  import Method._
  import Version._

  def svc(p: Promise[Request]): Service[Request, Nothing] = Service.mk { (req: Request) =>
    p.setValue(req)
    Future.never
  }

  test("module") {
    val host = "test.host"
    val port = 1234
    val p = new Promise[Request]
    val stk = LoadBalancedHostFilter.module.toStack(
      Stack.leaf(LoadBalancedHostFilter.role, ServiceFactory.const(svc(p))))
    val fac = stk.make(
      Stack.Params.empty + Transporter.EndpointAddr(
        Address.Inet(InetSocketAddress.createUnresolved(host, port), Map.empty))
    )
    Await.result(fac(), 1.second)(Request(Http11, Get, "/"))
    assert(Await.result[Request](p, 1.second).headerMap.get("Host").contains(host))
  }
}
