package com.twitter.finagle

import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AddressTest extends FunSuite {
  test("constructor with no host points to localhost") {
    Address(8080) == Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8080))
  }
}
