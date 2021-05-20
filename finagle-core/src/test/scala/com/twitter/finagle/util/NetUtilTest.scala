package com.twitter.finagle.util

import com.twitter.finagle.core.util.NetUtil
import org.scalatest.funsuite.AnyFunSuite

class NetUtilTest extends AnyFunSuite {
  test("NetUtil should isIpv4Address") {
    import NetUtil.isIpv4Address

    for (i <- 0.to(255)) {
      assert(isIpv4Address("%d.0.0.0".format(i)))
      assert(isIpv4Address("0.%d.0.0".format(i)))
      assert(isIpv4Address("0.0.%d.0".format(i)))
      assert(isIpv4Address("0.0.0.%d".format(i)))
      assert(isIpv4Address("%d.%d.%d.%d".format(i, i, i, i)))
    }

    assert(!isIpv4Address(""))
    assert(!isIpv4Address("no"))
    assert(!isIpv4Address("::127.0.0.1"))
    assert(!isIpv4Address("-1.0.0.0"))
    assert(!isIpv4Address("256.0.0.0"))
    assert(!isIpv4Address("0.256.0.0"))
    assert(!isIpv4Address("0.0.256.0"))
    assert(!isIpv4Address("0.0.0.256"))
  }
}
