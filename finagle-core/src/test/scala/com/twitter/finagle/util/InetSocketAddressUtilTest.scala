package com.twitter.finagle.util

import com.twitter.finagle.WeightedSocketAddress
import com.twitter.util.{Await, RandomSocket}
import java.net.{InetAddress, UnknownHostException, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InetSocketAddressUtilTest extends FunSuite {
  val port1 = RandomSocket.nextPort()
  val port2 = RandomSocket.nextPort()
  val weight1: Double = 0.5
  val weight2: Double = 0.25

  test("toPublic") {
    try {
      val myAddr = InetAddress.getLocalHost
      val mySockAddr = new InetSocketAddress(myAddr, port1)
      val inaddr_any = InetAddress.getByName("0.0.0.0")
      val boundSock = new InetSocketAddress(inaddr_any, port1)
      val loopback = InetAddress.getByName("127.0.0.1")
      val boundLoopback = new InetSocketAddress(loopback, port1)
      val ipv6loopback = InetAddress.getByName("::1")
      val boundIpv6Lo = new InetSocketAddress(ipv6loopback, port1)
      val ipv6any = InetAddress.getByName("::0")
      val boundIpv6Any = new InetSocketAddress(ipv6any, port1)

      assert(InetSocketAddressUtil.toPublic(mySockAddr) === mySockAddr)
      assert(InetSocketAddressUtil.toPublic(boundSock) === mySockAddr)
      assert(InetSocketAddressUtil.toPublic(boundIpv6Any) === mySockAddr)

      // It's ok if this test fails due to some future change, I just want to highlight it
      // to whoever re-implements toPublic in case they change the behavior
      assert(InetSocketAddressUtil.toPublic(boundLoopback) === boundLoopback)
      assert(InetSocketAddressUtil.toPublic(boundIpv6Lo) === boundIpv6Lo)
    }
    catch {
      // this could happen if you don't have a resolvable hostname or a public ip
      case e: UnknownHostException => info("Skipping tests because your network is misconfigured")
    }
  }

  test("resolveHostPorts") {
    assert(InetSocketAddressUtil.resolveHostPorts(Seq()).isEmpty)
    intercept[UnknownHostException] { InetSocketAddressUtil.resolveHostPorts(Seq(("gobble-d-gook", port1))) }

    assert(InetSocketAddressUtil.resolveHostPorts(Seq(("127.0.0.1", port1))) === Set(new InetSocketAddress("127.0.0.1", port1)))
    assert(InetSocketAddressUtil.resolveHostPorts(Seq(("127.0.0.1", port1), ("127.0.0.1", port2))) ===
      Set(new InetSocketAddress("127.0.0.1", port1), new InetSocketAddress("127.0.0.1", port2)))
  }

  test("parseHosts") {
    assert(InetSocketAddressUtil.parseHosts("").isEmpty)
    assert(InetSocketAddressUtil.parseHosts(",").isEmpty)
    intercept[IllegalArgumentException] { InetSocketAddressUtil.parseHosts("gobble-d-gook") }

    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:" + port1) === Seq(new InetSocketAddress("127.0.0.1", port1)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:" + port1) === Seq(new InetSocketAddress("127.0.0.1", port1)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:" + port1 + ",") === Seq(new InetSocketAddress("127.0.0.1", port1)))
    assert(InetSocketAddressUtil.parseHosts(",127.0.0.1:" + port1 + ",") === Seq(new InetSocketAddress("127.0.0.1", port1)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:" + port1 + " ") === Seq(new InetSocketAddress("127.0.0.1", port1)))
    assert(InetSocketAddressUtil.parseHosts(" 127.0.0.1:" + port1 + " ") === Seq(new InetSocketAddress("127.0.0.1", port1)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:" + port1 + ",127.0.0.1:" + port2) ===
      Seq(new InetSocketAddress("127.0.0.1", port1), new InetSocketAddress("127.0.0.1", port2)))
    assert(InetSocketAddressUtil.parseHosts("127.0.0.1:" + port1 + " 127.0.0.1:" + port2) ===
      Seq(new InetSocketAddress("127.0.0.1", port1), new InetSocketAddress("127.0.0.1", port2)))

    assert(InetSocketAddressUtil.parseHosts(":" + port1) === Seq(new InetSocketAddress("0.0.0.0", port1)))
  }

  test("resolveWeightedHostPorts") {
    assert(Await.result(InetSocketAddressUtil.resolveWeightedHostPorts(Seq())).isEmpty)
    intercept[UnknownHostException] {
      Await.result(InetSocketAddressUtil.resolveWeightedHostPorts(Seq(("gobble-d-gook", port1, weight1))))
    }

    assert(Await.result(InetSocketAddressUtil.resolveWeightedHostPorts(Seq(("127.0.0.1", port1, weight1)))) ===
      Seq(WeightedSocketAddress(new InetSocketAddress("127.0.0.1", port1), weight1)))
    assert(Await.result(InetSocketAddressUtil.resolveWeightedHostPorts(
      Seq(("127.0.0.1", port1, weight1), ("127.0.0.1", port2, weight2)))) ===
      Seq(
        WeightedSocketAddress(new InetSocketAddress("127.0.0.1", port1), weight1),
        WeightedSocketAddress(new InetSocketAddress("127.0.0.1", port2), weight2)))
  }
}
