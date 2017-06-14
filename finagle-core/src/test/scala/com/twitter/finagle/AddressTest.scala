package com.twitter.finagle

import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite

class AddressTest extends FunSuite {
  test("constructor with no host points to localhost") {
    Address(8080) == Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8080))
  }
}
