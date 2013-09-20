package com.twitter.finagle.util

import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InetSocketAddressUtilTest extends FunSuite {
  test("parseHosts") {
    assert(InetSocketAddressUtil.parseHosts("").isEmpty)
    assert(InetSocketAddressUtil.parseHosts(",").isEmpty)
    intercept[IllegalArgumentException] { InetSocketAddressUtil.parseHosts("gobble-d-gook") }

    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:11211") === Seq(new InetSocketAddress("127.0.0.1", 11211)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:11211") === Seq(new InetSocketAddress("127.0.0.1", 11211)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:11211,") === Seq(new InetSocketAddress("127.0.0.1", 11211)))
    assert(InetSocketAddressUtil.parseHosts(",127.0.0.1:11211,") === Seq(new InetSocketAddress("127.0.0.1", 11211)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:11211 ") === Seq(new InetSocketAddress("127.0.0.1", 11211)))
    assert(InetSocketAddressUtil.parseHosts(" 127.0.0.1:11211 ") === Seq(new InetSocketAddress("127.0.0.1", 11211)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:11211,127.0.0.1:11212") ===
      Seq(new InetSocketAddress("127.0.0.1", 11211), new InetSocketAddress("127.0.0.1", 11212)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:11211 127.0.0.1:11212") ===
      Seq(new InetSocketAddress("127.0.0.1", 11211), new InetSocketAddress("127.0.0.1", 11212)))

    assert(InetSocketAddressUtil.parseHosts(":11211") === Seq(new InetSocketAddress("0.0.0.0", 11211)))
  }
}
