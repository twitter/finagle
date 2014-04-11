package com.twitter.finagle.socks

import com.twitter.util.RandomSocket
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class SocksProxyFlagsTest extends FunSuite with BeforeAndAfter {

  test("SocksProxyFlags should respect -socksProxyHost / -socksProxyPort flags") {
    val port = RandomSocket.nextPort()
    System.setProperty("socksProxyHost", "localhost")
    System.setProperty("socksProxyPort", port.toString)

    assert(SocksProxyFlags.socksProxy === Some(new InetSocketAddress("localhost", port)))
  }

  test("SocksProxyFlags should respect missing -socksProxyHost / -socksProxyPort flags") {
    assert(SocksProxyFlags.socksProxy === None)
  }

  after {
    System.clearProperty("socksProxyHost")
    System.clearProperty("socksProxyPort")
  }
}
