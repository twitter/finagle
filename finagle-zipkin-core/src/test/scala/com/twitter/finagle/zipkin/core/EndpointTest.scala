package com.twitter.finagle.zipkin.core

import java.net.InetSocketAddress
import org.scalatest.FunSuite

class EndpointTest extends FunSuite {

  private[this] val unresolved = InetSocketAddress.createUnresolved("nope", 44)

  test("toIpv4 with null") {
    assert(0 == Endpoint.toIpv4(unresolved.getAddress))
  }

  test("fromSocketAddress with unresolved InetSocketAddress") {
    val endpoint = Endpoint.fromSocketAddress(unresolved)
    assert(0 == endpoint.ipv4)
    assert(unresolved.getPort == endpoint.port)
  }

}
