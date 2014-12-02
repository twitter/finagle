package com.twitter.finagle

import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WeightedSocketAddressTest extends FunSuite {
  test("WeightedSocketAddress.Impl") {
    val sa = new SocketAddress {}
    val wsa = WeightedSocketAddress(sa, 1.2)
    val WeightedSocketAddress(`sa`, 1.2) = wsa
    val WeightedSocketAddress(`sa`, 1.0) = sa
  }

  test("WeightedInetSocketAddress") {
    val ia = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val wsa = WeightedSocketAddress(ia, 8.9)
    val WeightedSocketAddress(`ia`, 8.9) = wsa
    val WeightedSocketAddress(`ia`, 1.0) = ia
    val WeightedInetSocketAddress(`ia`, 8.9) = wsa
    val WeightedInetSocketAddress(`ia`, 1.0) = ia
  }
}
