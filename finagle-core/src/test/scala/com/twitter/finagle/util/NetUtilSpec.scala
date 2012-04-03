package com.twitter.finagle.core.util

import org.specs.SpecificationWithJUnit


class NetUtilSpec extends SpecificationWithJUnit {

  "NetUtil" should {
    "isIpv4Address" in {
      import NetUtil.isIpv4Address

      for (i <- 0.to(255)) {
        isIpv4Address("%d.0.0.0".format(i)) must beTrue
        isIpv4Address("0.%d.0.0".format(i)) must beTrue
        isIpv4Address("0.0.%d.0".format(i)) must beTrue
        isIpv4Address("0.0.0.%d".format(i)) must beTrue
        isIpv4Address("%d.%d.%d.%d".format(i, i, i, i)) must beTrue
      }

      isIpv4Address("")            must beFalse
      isIpv4Address("no")          must beFalse
      isIpv4Address("::127.0.0.1") must beFalse
      isIpv4Address("-1.0.0.0")    must beFalse
      isIpv4Address("256.0.0.0")   must beFalse
      isIpv4Address("0.256.0.0")   must beFalse
      isIpv4Address("0.0.256.0")   must beFalse
      isIpv4Address("0.0.0.256")   must beFalse
    }
  }
}
