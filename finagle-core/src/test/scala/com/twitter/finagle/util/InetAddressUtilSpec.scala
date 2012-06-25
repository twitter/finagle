package com.twitter.finagle.core.util

import org.specs.SpecificationWithJUnit
import java.net.InetAddress


class InetAddressUtilSpec extends SpecificationWithJUnit {

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

    "getByName" in {
      import InetAddressUtil.getByName
      getByName("69.55.236.117").getHostAddress   must_== "69.55.236.117"
      getByName("0.0.0.0").getHostAddress         must_== "0.0.0.0"
      getByName("255.0.0.0").getHostAddress       must_== "255.0.0.0"
      getByName("0.255.0.0").getHostAddress       must_== "0.255.0.0"
      getByName("0.0.255.0").getHostAddress       must_== "0.0.255.0"
      getByName("0.0.0.255").getHostAddress       must_== "0.0.0.255"
      getByName("255.255.255.255").getHostAddress must_== "255.255.255.255"
    }
  }
}
