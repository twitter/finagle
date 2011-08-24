package com.twitter.finagle.core.util

import org.specs.Specification
import java.net.InetAddress


object InetAddressUtilSpec extends Specification {

  "InetAddressUtil" should {
    "isPrivateAddress" in {
      import InetAddressUtil.isPrivateAddress
      isPrivateAddress(InetAddress.getByName("0.0.0.0"))         must beFalse
      isPrivateAddress(InetAddress.getByName("199.59.148.13"))   must beFalse
      isPrivateAddress(InetAddress.getByName("10.0.0.0"))        must beTrue
      isPrivateAddress(InetAddress.getByName("10.255.255.255"))  must beTrue
      isPrivateAddress(InetAddress.getByName("172.16.0.0"))      must beTrue
      isPrivateAddress(InetAddress.getByName("172.31.255.255"))  must beTrue
      isPrivateAddress(InetAddress.getByName("192.168.0.0"))     must beTrue
      isPrivateAddress(InetAddress.getByName("192.168.255.255")) must beTrue
    }
  }
}
