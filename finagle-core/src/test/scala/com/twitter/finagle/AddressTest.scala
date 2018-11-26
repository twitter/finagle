package com.twitter.finagle

import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite

class AddressTest extends FunSuite {
  test("constructor with no host points to localhost") {
    assert(Address(8080) == Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8080)))
  }

  test("hashOrdering") {
    val addr1 = Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8080))
    val addr2 = Address(InetSocketAddress.createUnresolved("btbb", 10))
    val addr3 = Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8090))
    val ordering = Address.hashOrdering(System.currentTimeMillis().toInt)

    val sorted = Seq(addr1, addr2, addr3).toVector.sorted(ordering)

    val cmp13 = ordering.compare(sorted(0), sorted(2))
    val cmp12 = ordering.compare(sorted(0), sorted(1))
    val cmp23 = ordering.compare(sorted(1), sorted(2))
    assert(cmp13 == 0 && cmp12 == 0 && cmp23 == 0 || cmp13 < 0 && (cmp12 < 0 || cmp23 < 0))

  }
}
