package com.twitter.finagle.memcached.integration

import _root_.java.lang.{Boolean => JBoolean}
import java.net.{InetAddress, SocketAddress, InetSocketAddress}
import com.twitter.finagle.memcached.{CacheNodeGroup, KetamaClientBuilder}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.{Group, Name}
import com.twitter.io.Charsets
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class KetamaClientTest extends FunSuite with BeforeAndAfter {
  /**
    * We already proved above that we can hit a real memcache server,
    * so we can use our own for the partitioned client test.
    */
  var server1: InProcessMemcached = null
  var server2: InProcessMemcached = null
  var address1: InetSocketAddress = null
  var address2: InetSocketAddress = null

  before {
    server1 = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    address1 = server1.start().localAddress.asInstanceOf[InetSocketAddress]
    server2 = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    address2 = server2.start().localAddress.asInstanceOf[InetSocketAddress]
  }

  after {
    server1.stop()
    server2.stop()
  }

  test("doesn't blow up") {
    val client = KetamaClientBuilder()
      .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
      .build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("using Name doesn't blow up") {
    val name = Name.bound(address1, address2)
    val client = KetamaClientBuilder().dest(name).build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("using Group[InetSocketAddress] doesn't blow up") {
    val mutableGroup = Group(address1, address2).map{_.asInstanceOf[SocketAddress]}
    val client = KetamaClientBuilder()
      .group(CacheNodeGroup(mutableGroup, true))
      .build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("using custom keys doesn't blow up") {
    val client = KetamaClientBuilder()
      .nodes("localhost:%d:1:key1,localhost:%d:1:key2".format(address1.getPort, address2.getPort))
      .build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) === None)
    Await.result(client.set("foo", "bar"))
    assert(Await.result(client.get("foo")).get.toString(Charsets.Utf8) === "bar")
  }

  test("even in future pool") {
    lazy val client = KetamaClientBuilder()
      .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
      .build()

    val futureResult = Future.value(true) flatMap {
      _ => client.get("foo")
    }

    assert(Await.result(futureResult) === None)
  }
}
