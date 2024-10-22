package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.integration.external.TestMemcachedServer
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.{param => ctfparam}
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.util._
import java.net.InetAddress
import java.net.InetSocketAddress

class MemcachedPartitioningClientTest extends MemcachedTest {

  protected def createClient(dest: Name, clientName: String): Client = {
    Memcached.client.newRichClient(dest, clientName)
  }

  protected[this] val redistributesKey: Seq[String] =
    Seq("test_client", "partitioner", "redistributes")
  protected[this] val leavesKey: Seq[String] = Seq(clientName, "partitioner", "leaves")
  protected[this] val revivalsKey: Seq[String] = Seq(clientName, "partitioner", "revivals")
  protected[this] val ejectionsKey: Seq[String] = Seq(clientName, "partitioner", "ejections")

  test("re-hash when a bad host is ejected") {
    val sr = new InMemoryStatsReceiver
    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .configured(TimeoutFilter.Param(10000.milliseconds))
      .configured(param.EjectFailedHost(true))
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(ctfparam.Stats(sr))
      .newRichClient(Name.bound(servers.map { s => Address(s.address) }: _*), clientName)
    testRehashUponEject(client, sr)
    client.close()
  }

  test("host comes back into ring after being ejected") {
    testRingReEntryAfterEjection((timer, cacheServer, statsReceiver) =>
      Memcached.client
        .configured(param.KeyHasher(KeyHasher.FNV1_32))
        .configured(TimeoutFilter.Param(10000.milliseconds))
        .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
        .configured(param.EjectFailedHost(true))
        .configured(ctfparam.Timer(timer))
        .configured(ctfparam.Stats(statsReceiver))
        .newRichClient(
          Name.bound(Address(cacheServer.boundAddress.asInstanceOf[InetSocketAddress])),
          clientName))
  }

  test("Add and remove nodes") {
    val addrs = servers.map { s => Address(s.address) }

    // Start with 3 backends
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound(addrs.toSet.drop(2)))

    val sr = new InMemoryStatsReceiver

    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .configured(TimeoutFilter.Param(10000.milliseconds))
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(param.EjectFailedHost(true))
      .connectionsPerEndpoint(NumConnections)
      .withStatsReceiver(sr)
      .newRichClient(Name.Bound.singleton(mutableAddrs), clientName)
    testAddAndRemoveNodes(addrs, mutableAddrs, sr)
    client.close()
  }

  test("FailureAccrualFactoryException has remote address") {
    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .configured(TimeoutFilter.Param(10000.milliseconds))
      .configured(FailureAccrualFactory.Param(1, 10.minutes))
      .configured(param.EjectFailedHost(false))
      .connectionsPerEndpoint(1)
      .newRichClient(Name.bound(Address("localhost", 1234)), clientName)
    testFailureAccrualFactoryExceptionHasRemoteAddress(client)
    client.close()
  }

  if (!Option(System.getProperty("EXTERNAL_MEMCACHED_PATH")).isDefined) {
    test("traces fanout requests") {
      // we use an eventually block to retry the request if we didn't get partitioned to different shards
      eventually {
        val tracer = new BufferingTracer()

        // the servers created in MemcachedTest have inconsistent addresses, which means sharding
        // will be inconsistent across runs. To combat this, we'll start our own servers and rerun the
        // tests if we partition to the same shard.
        val serverOpts =
          for (_ <- 1 to NumServers)
            yield TestMemcachedServer.start(
              Some(new InetSocketAddress(InetAddress.getLoopbackAddress, 0)))
        val servers: Seq[TestMemcachedServer] = serverOpts.flatten

        val client = Memcached.client
          .configured(param.KeyHasher(KeyHasher.FNV1_32))
          .connectionsPerEndpoint(1)
          .withTracer(tracer)
          .newRichClient(Name.bound(servers.map { s => Address(s.address) }: _*), clientName)

        awaitResult(client.set("foo", Buf.Utf8("bar")))
        awaitResult(client.set("baz", Buf.Utf8("boing")))
        awaitResult(
          client.gets(Seq("foo", "baz"))
        ).flatMap {
          case (key, (Buf.Utf8(value1), Buf.Utf8(value2))) =>
            Map((key, (value1, value2)))
        }

        client.close()
        servers.foreach(_.stop())

        val gets: Seq[TraceId] = tracer.iterator.toList collect {
          case Record(id, _, Annotation.Rpc("Gets"), _) => id
        }

        // Moving the MemcachedTracingFilter means that partitioned requests should result in two gets spans
        assert(gets.length == 2)
        // However the FanoutProxy should ensure that the requests are stored in peers, not the same tid.
        gets.tail.foreach { get =>
          assert(get._parentId == gets.head._parentId)
          assert(get.spanId != gets.head.spanId)
        }
      }
    }
  }

  test("data read/write consistency between old and new clients") {
    testCompatibility()
  }
}
