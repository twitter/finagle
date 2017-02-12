package com.twitter.finagle.socks

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class SocksProxyFlagsTest extends FunSuite with BeforeAndAfter {

  test("SocksProxyFlags should respect -socksProxyHost / -socksProxyPort flags") {
    val port = 80 // never bound
    socksProxyHost.let("localhost") {
      socksProxyPort.let(port) {
        assert(SocksProxyFlags.socksProxy == Some(new InetSocketAddress("localhost", port)))
      }
    }
  }

  test("SocksProxyFlags should respect missing -socksProxyHost / -socksProxyPort flags") {
    assert(SocksProxyFlags.socksProxy == None)
  }
}
