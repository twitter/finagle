package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{param => ctfparam, _}
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.memcached.{Client, TwemcacheClient}
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.ReadWriteVar
import java.net.InetSocketAddress

class MemcachedUtilityTest extends MemcachedTest {

  protected[this] val redistributesKey: Seq[String] =
    Seq("test_client", "partitioner", "redistributes")
  protected[this] val leavesKey: Seq[String] = Seq(clientName, "partitioner", "leaves")
  protected[this] val revivalsKey: Seq[String] = Seq(clientName, "partitioner", "revivals")
  protected[this] val ejectionsKey: Seq[String] = Seq(clientName, "partitioner", "ejections")

  protected def createClient(dest: Name, clientName: String): Client = {
    newPartitioningClient(dest)
  }

  private[this] def newPartitioningClient(
    dest: Name,
    label: String = clientName,
    create: (Name, String) => Client = defaultCreate
  ): Client = {
    var client: Client = null
    client = create(dest, label)
    assert(client.isInstanceOf[TwemcacheClient]) // assert new client
    client
  }

  private[this] def defaultCreate(dest: Name, label: String): Client = {
    Memcached.client.newRichClient(dest, label)
  }

  test("re-hash when a bad host is ejected") {
    val sr = new InMemoryStatsReceiver
    val dest = Name.bound(servers.map { s => Address(s.address) }: _*)
    val client = newPartitioningClient(
      dest,
      clientName,
      (dest, clientName) => {
        Memcached.client
          .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
          .configured(param.EjectFailedHost(true))
          .withStatsReceiver(sr)
          .newRichClient(dest, clientName)
      }
    )
    testRehashUponEject(client, sr)
    client.close()
  }

  test("host comes back into ring after being ejected") {
    testRingReEntryAfterEjection((timer, cacheServer, statsReceiver) => {
      val dest: Name =
        Name.bound(Address(cacheServer.boundAddress.asInstanceOf[InetSocketAddress]))
      val client: Client = newPartitioningClient(
        dest,
        clientName,
        (dest, clientName) => {
          Memcached.client
            .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
            .configured(param.EjectFailedHost(true))
            .configured(ctfparam.Timer(timer))
            .configured(ctfparam.Stats(statsReceiver))
            .newRichClient(dest, clientName)
        }
      )
      client
    })
  }

  test("Add and remove nodes") {
    val addrs = servers.map { s => Address(s.address) }

    // Start with 3 backends
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound(addrs.toSet.drop(2)))

    val sr = new InMemoryStatsReceiver
    val dest = Name.Bound.singleton(mutableAddrs)
    val clientName = "test_client"
    val client = newPartitioningClient(
      dest,
      clientName,
      (dest, clientName) => {
        Memcached.client
          .connectionsPerEndpoint(NumConnections)
          .withStatsReceiver(sr)
          .newRichClient(dest, clientName)
      }
    )
    testAddAndRemoveNodes(addrs, mutableAddrs, sr)
    client.close()
  }

  test("FailureAccrualFactoryException has remote address") {
    val dest = Name.bound(Address("localhost", 1234))
    val label = "client"
    val client = newPartitioningClient(
      dest,
      clientName,
      (dest, clientName) => {
        Memcached.client
          .connectionsPerEndpoint(1)
          // 1 failure triggers FA; make sure FA stays in "dead" state after failure
          .configured(FailureAccrualFactory.Param(1, 10.minutes))
          .withEjectFailedHost(false)
          .newTwemcacheClient(dest, clientName)
      }
    )
    testFailureAccrualFactoryExceptionHasRemoteAddress(client)
    client.close()
  }

  test("data read/write consistency between old and new clients") {
    testCompatibility()
  }
}
