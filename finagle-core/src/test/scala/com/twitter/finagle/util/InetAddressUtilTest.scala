package com.twitter.finagle.util

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.twitter.finagle.core.util.InetAddressUtil
import java.net.InetAddress

@RunWith(classOf[JUnitRunner])
class InetAddressUtilTest extends FunSuite {

  test("InetAddressUtil should isPrivateAddress") {
    import InetAddressUtil.isPrivateAddress
    assert(!isPrivateAddress(InetAddress.getByName("0.0.0.0")))
    assert(!isPrivateAddress(InetAddress.getByName("199.59.148.13")))
    assert(isPrivateAddress(InetAddress.getByName("10.0.0.0")))
    assert(isPrivateAddress(InetAddress.getByName("10.255.255.255")))
    assert(isPrivateAddress(InetAddress.getByName("172.16.0.0")))
    assert(isPrivateAddress(InetAddress.getByName("172.31.255.255")))
    assert(isPrivateAddress(InetAddress.getByName("192.168.0.0")))
    assert(isPrivateAddress(InetAddress.getByName("192.168.255.255")))
  }

  test("InetAddressUtil should getByName") {
    import InetAddressUtil.getByName
    assert(getByName("69.55.236.117").getHostAddress == "69.55.236.117")
    assert(getByName("0.0.0.0").getHostAddress == "0.0.0.0")
    assert(getByName("255.0.0.0").getHostAddress == "255.0.0.0")
    assert(getByName("0.255.0.0").getHostAddress == "0.255.0.0")
    assert(getByName("0.0.255.0").getHostAddress == "0.0.255.0")
    assert(getByName("0.0.0.255").getHostAddress == "0.0.0.255")
    assert(getByName("255.255.255.255").getHostAddress == "255.255.255.255")
  }
}
