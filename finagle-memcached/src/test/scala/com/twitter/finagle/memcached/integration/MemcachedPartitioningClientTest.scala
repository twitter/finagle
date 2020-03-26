package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{param => ctfparam, _}
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.memcached.partitioning.MemcachedPartitioningService
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.memcached.{Client, TwemcacheClient}
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.hashing.KeyHasher
import com.twitter.util._
import java.net.InetSocketAddress

class MemcachedPartitioningClientTest extends MemcachedTest {

  protected[this] val isOldClient: Boolean = false

  protected def baseClient: Memcached.Client = Memcached.client

  protected def createClient(dest: Name, clientName: String): Client = {
    newClient(dest)
  }

  protected[this] val redistributesKey: Seq[String] =
    Seq("test_client", "partitioner", "redistributes")
  protected[this] val leavesKey: Seq[String] = Seq(clientName, "partitioner", "leaves")
  protected[this] val revivalsKey: Seq[String] = Seq(clientName, "partitioner", "revivals")
  protected[this] val ejectionsKey: Seq[String] = Seq(clientName, "partitioner", "ejections")

  private[this] def newClientStack(): Stack[ServiceFactory[Command, Response]] = {
    // create a partitioning aware finagle client by inserting the PartitioningService appropriately
    StackClient
      .newStack[Command, Response]
      .insertAfter(
        BindingFactory.role,
        MemcachedPartitioningService.module
      )
  }

  private[this] def newClient(dest: Name, label: String = clientName) = {
    TwemcacheClient(
      baseClient
        .configured(param.KeyHasher(KeyHasher.KETAMA))
        .configured(TimeoutFilter.Param(10000.milliseconds))
        .configured(param.EjectFailedHost(false))
        .configured(LoadBalancerFactory.ReplicateAddresses(2))
        .withStack(newClientStack())
        .newService(dest, label)
    )
  }

  test("re-hash when a bad host is ejected") {
    val sr = new InMemoryStatsReceiver
    val client = TwemcacheClient(
      Memcached.client
        .configured(param.KeyHasher(KeyHasher.FNV1_32))
        .configured(TimeoutFilter.Param(10000.milliseconds))
        .configured(param.EjectFailedHost(true))
        .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
        .configured(ctfparam.Stats(sr))
        .withStack(newClientStack())
        .newService(Name.bound(servers.map { s => Address(s.address) }: _*), clientName)
    )
    testRehashUponEject(client, sr)
    client.close()
  }

  test("host comes back into ring after being ejected") {
    testRingReEntryAfterEjection((timer, cacheServer, statsReceiver) =>
      TwemcacheClient(
        Memcached.client
          .configured(param.KeyHasher(KeyHasher.FNV1_32))
          .configured(TimeoutFilter.Param(10000.milliseconds))
          .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
          .configured(param.EjectFailedHost(true))
          .configured(ctfparam.Timer(timer))
          .configured(ctfparam.Stats(statsReceiver))
          .withStack(newClientStack())
          .newService(
            Name.bound(Address(cacheServer.boundAddress.asInstanceOf[InetSocketAddress])),
            clientName
          )
      ))
  }

  test("Add and remove nodes") {
    val addrs = servers.map { s => Address(s.address) }

    // Start with 3 backends
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound(addrs.toSet.drop(2)))

    val sr = new InMemoryStatsReceiver

    val client = TwemcacheClient(
      Memcached.client
        .configured(param.KeyHasher(KeyHasher.FNV1_32))
        .configured(TimeoutFilter.Param(10000.milliseconds))
        .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
        .configured(param.EjectFailedHost(true))
        .connectionsPerEndpoint(NumConnections)
        .withStack(newClientStack())
        .withStatsReceiver(sr)
        .newService(
          Name.Bound.singleton(mutableAddrs),
          clientName
        )
    )
    testAddAndRemoveNodes(addrs, mutableAddrs, sr)
    client.close()
  }

  test("FailureAccrualFactoryException has remote address") {
    val client = TwemcacheClient(
      Memcached.client
        .configured(param.KeyHasher(KeyHasher.FNV1_32))
        .configured(TimeoutFilter.Param(10000.milliseconds))
        .configured(FailureAccrualFactory.Param(1, 10.minutes))
        .configured(param.EjectFailedHost(false))
        .connectionsPerEndpoint(1)
        .withStack(newClientStack())
        .newService(Name.bound(Address("localhost", 1234)), clientName)
    )
    testFailureAccrualFactoryExceptionHasRemoteAddress(client)
    client.close()
  }

  test("data read/write consistency between old and new clients") {
    testCompatibility()
  }
}
