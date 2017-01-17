package com.twitter.finagle.memcached.integration

import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.loadbalancer.ConcurrentLoadBalancerFactory
import com.twitter.finagle.memcached.{CacheNodeGroup, KetamaClientBuilder, KetamaPartitionedClient, KetamaClientKey}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.ReadWriteVar
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
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
    address1 = server1.start().boundAddress.asInstanceOf[InetSocketAddress]
    server2 = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    address2 = server2.start().boundAddress.asInstanceOf[InetSocketAddress]
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
    assert(Await.result(client.get("foo")) == None)

    Await.result(client.set("foo", Buf.Utf8("bar")))
    val Buf.Utf8(res) = Await.result(client.get("foo")).get
    assert(res == "bar")
  }

  test("using Name doesn't blow up") {
    val name = Name.bound(Address(address1), Address(address2))
    val client = KetamaClientBuilder().dest(name).build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    val Buf.Utf8(res) = Await.result(client.get("foo")).get
    assert(res == "bar")
  }
  
  test("using .dest() preserves custom keys") {
    val key1 = 0
    val key2 = 3
    val name = s"twcache!localhost:${address1.getPort}:1:$key1,localhost:${address2.getPort}:1:$key2"
    val client = KetamaClientBuilder().dest(name).build().asInstanceOf[KetamaPartitionedClient]
    
    assert(client.ketamaNodes.size == 2)
    assert(client.ketamaNodes.map(_._1) == Set(KetamaClientKey(key1.toString), KetamaClientKey(key2.toString)))

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    val Buf.Utf8(res) = Await.result(client.get("foo")).get
    assert(res == "bar")
  }

  test("using Group[InetSocketAddress] doesn't blow up") {
    val mutableGroup = Group(address1, address2).map{_.asInstanceOf[SocketAddress]}
    val client = KetamaClientBuilder()
      .group(CacheNodeGroup(mutableGroup, true))
      .build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))
    val Buf.Utf8(res) = Await.result(client.get("foo")).get
    assert(res == "bar")
  }

  test("using custom keys doesn't blow up") {
    val client = KetamaClientBuilder()
      .nodes("localhost:%d:1:key1,localhost:%d:1:key2".format(address1.getPort, address2.getPort))
      .build()

    Await.result(client.delete("foo"))
    assert(Await.result(client.get("foo")) == None)
    Await.result(client.set("foo", Buf.Utf8("bar")))

    val Buf.Utf8(res) = Await.result(client.get("foo")).get
    assert(res == "bar")
  }

  test("even in future pool") {
    lazy val client = KetamaClientBuilder()
      .nodes("localhost:%d,localhost:%d".format(address1.getPort, address2.getPort))
      .build()

    val futureResult = Future.value(true) flatMap {
      _ => client.get("foo")
    }

    assert(Await.result(futureResult) == None)
  }

  test("using Group respects updates") {
    val servers = for (_ <- 1 to 5)
      yield new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))

    val addrs = servers.map { s => (Address(s.start().boundAddress.asInstanceOf[InetSocketAddress])) }

    // Start with 3 backends
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound(addrs.toSet.drop(2)))
    val group = CacheNodeGroup(Group.fromVarAddr(mutableAddrs))
    val sr = new InMemoryStatsReceiver
    val myClient = KetamaClientBuilder()
      .group(group)
      .clientBuilder(
        ClientBuilder()
          .name("test_client")
          .reportTo(sr)
          .hostConnectionLimit(1))
      .build()

    assert(sr.counters(Seq("test_client", "memcached_client", "redistributes")) == 1)
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == 0)

    // Add 2 nodes to the backends, for a total of 5 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet))

    assert(sr.counters(Seq("test_client", "memcached_client", "redistributes")) == 2)
    // Need to rebuild each of the 5 nodes with `numConnections`
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == 0)
  }
}
