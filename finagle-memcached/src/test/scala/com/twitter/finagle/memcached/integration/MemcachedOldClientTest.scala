package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.param.{Stats, Timer}
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.partitioning.param.EjectFailedHost
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import java.net.InetSocketAddress

class MemcachedOldClientTest extends MemcachedTest {

  protected def createClient(dest: Name, clientName: String): Client = {
    Memcached.client.newRichClient(dest, clientName)
  }

  protected[this] val redistributesKey: Seq[String] =
    Seq(clientName, "partitioner", "redistributes")
  protected[this] val leavesKey: Seq[String] = Seq(clientName, "partitioner", "leaves")
  protected[this] val revivalsKey: Seq[String] = Seq(clientName, "partitioner", "revivals")
  protected[this] val ejectionsKey: Seq[String] = Seq(clientName, "partitioner", "ejections")

  test("re-hash when a bad host is ejected") {
    val sr = new InMemoryStatsReceiver
    val client = Memcached.client
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(param.EjectFailedHost(true))
      .withStatsReceiver(sr)
      .newTwemcacheClient(Name.bound(servers.map { s => Address(s.address) }: _*), clientName)
    testRehashUponEject(client, sr)
    client.close()
  }

  test("host comes back into ring after being ejected") {
    testRingReEntryAfterEjection((timer, cacheServer, statsReceiver) =>
      Memcached.client
        .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
        .configured(EjectFailedHost(true))
        .configured(Timer(timer))
        .configured(Stats(statsReceiver))
        .newTwemcacheClient(
          Name.bound(Address(cacheServer.boundAddress.asInstanceOf[InetSocketAddress])),
          clientName
        ))
  }

  test("Add and remove nodes") {
    val addrs = servers.map { s => Address(s.address) }

    // Start with 3 backends
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound(addrs.toSet.drop(2)))

    val sr = new InMemoryStatsReceiver

    val client = Memcached.client
      .connectionsPerEndpoint(NumConnections)
      .withStatsReceiver(sr)
      .newTwemcacheClient(Name.Bound.singleton(mutableAddrs), "test_client")

    testAddAndRemoveNodes(addrs, mutableAddrs, sr)
    client.close()
  }

  test("FailureAccrualFactoryException has remote address") {
    val client = Memcached.client
      .connectionsPerEndpoint(1)
      // 1 failure triggers FA; make sure FA stays in "dead" state after failure
      .configured(FailureAccrualFactory.Param(1, 10.minutes))
      .withEjectFailedHost(false)
      .newTwemcacheClient(Name.bound(Address("localhost", 1234)), "client")
    testFailureAccrualFactoryExceptionHasRemoteAddress(client)
    client.close()
  }
}
