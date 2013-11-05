package com.twitter.finagle.util

import java.net.{InetAddress, UnknownHostException, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InetSocketAddressUtilTest extends FunSuite {
  test("toPublic") {
    try {
      val myAddr = InetAddress.getLocalHost
      val mySockAddr = new InetSocketAddress(myAddr, 1234)
      val inaddr_any = InetAddress.getByName("0.0.0.0")
      val boundSock = new InetSocketAddress(inaddr_any, 1234)
      val loopback = InetAddress.getByName("127.0.0.1")
      val boundLoopback = new InetSocketAddress(loopback, 1234)

      assert(InetSocketAddressUtil.toPublic(mySockAddr) === mySockAddr)
      assert(InetSocketAddressUtil.toPublic(boundSock) == mySockAddr)

      // It's ok if this test fails due to some future change, I just want to highlight it
      // to whoever re-implements toPublic in case they change the behavior
      assert(InetSocketAddressUtil.toPublic(boundLoopback) === boundLoopback)
    }
    catch {
      // this could happen if you don't have a resolvable hostname or a public ip
      case e: UnknownHostException => info("Skipping tests because your network is misconfigured")
    }
  }

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
