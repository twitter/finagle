package com.twitter.finagle.socks

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class SocksProxyFlagsTest extends FunSuite with BeforeAndAfter {

  test("SocksProxyFlags should respect -socksProxyHost / -socksProxyPort flags") {
    System.setProperty("socksProxyHost", "localhost")
    System.setProperty("socksProxyPort", "50001")

    assert(SocksProxyFlags.socksProxy === Some(new InetSocketAddress("localhost", 50001)))
  }

  test("SocksProxyFlags should respect missing -socksProxyHost / -socksProxyPort flags") {
    assert(SocksProxyFlags.socksProxy === None)
  }

  after {
    System.clearProperty("socksProxyHost")
    System.clearProperty("socksProxyPort")
  }
}
