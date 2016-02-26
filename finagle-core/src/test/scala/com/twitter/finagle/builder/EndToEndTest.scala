package com.twitter.finagle.builder

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.DefaultPool
import com.twitter.finagle.integration.{DynamicCluster, StringCodec}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.{Retries, RetryPolicy}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, CountDownLatch, Future, Promise}
import java.net.{InetAddress, SocketAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite {

  test("Finagle client should handle pending request after a host is deleted from cluster") {
    val constRes = new Promise[String]
    val arrivalLatch = new CountDownLatch(1)
    val service = new Service[String, String] {
      def apply(request: String) = {
        arrivalLatch.countDown()
        constRes
      }
    }
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(service)
    val cluster = new DynamicCluster[SocketAddress](Seq(server.boundAddress))
    val client = ClientBuilder()
      .cluster(cluster)
      .codec(StringCodec)
      .daemon(true) // don't create an exit guard
      .hostConnectionLimit(1)
      .build()

    // create a pending request; delete the server from cluster
    //  then verify the request can still finish
    val response = client("123")
    arrivalLatch.await()
    cluster.del(server.boundAddress)
    assert(!response.isDefined)
    constRes.setValue("foo")
    assert(Await.result(response, 1.second) == "foo")
  }

  test("Finagle client should queue requests while waiting for cluster to initialize") {
    val echo = new Service[String, String] {
      def apply(request: String) = Future.value(request)
    }
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(echo)

    // start with an empty cluster
    val cluster = new DynamicCluster[SocketAddress](Seq[SocketAddress]())
    val client = ClientBuilder()
      .cluster(cluster)
      .codec(StringCodec)
      .daemon(true) // don't create an exit guard
      .hostConnectionLimit(1)
      .hostConnectionMaxWaiters(5)
      .build()

    val responses = new Array[Future[String]](5)
    0 until 5 foreach { i =>
      responses(i) = client(i.toString)
      assert(!responses(i).isDefined)
    }

    // make cluster available, now queued requests should be processed
    val thread = new Thread {
      override def run = cluster.add(server.boundAddress)
    }

    cluster.ready.map { _ =>
      0 until 5 foreach { i =>
        assert(Await.result(responses(i), 1.second) == i.toString)
      }
    }
    thread.start()
    thread.join()
  }

  test("ClientBuilder should be properly instrumented on service application failure") {
    val never = new Service[String, String] {
      def apply(request: String) = new Promise[String]
    }
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(never)

    val mem = new InMemoryStatsReceiver
    val client = ClientBuilder()
      .name("client")
      .hosts(server.boundAddress)
      .codec(StringCodec)
      .daemon(true) // don't create an exit guard
      .requestTimeout(10.millisecond)
      .hostConnectionLimit(1)
      .hostConnectionMaxWaiters(1)
      .reportTo(mem)
      .build()

    // generate com.twitter.finagle.IndividualRequestTimeoutException
    intercept[IndividualRequestTimeoutException] { Await.result(client("hi"), 1.second) }
    Await.ready(server.close(), 1.second)

    val requestFailures = mem.counters(Seq("client", "failures"))
    val requeues =
      mem.counters.get(Seq("client", "retries", "requeues"))
    assert(requestFailures == 1)
    assert(requeues == None)
  }

  test("ClientBuilder should be properly instrumented on service acquisition failure") {
    val mem = new InMemoryStatsReceiver
    val client = ClientBuilder()
        .name("client")
        .hosts(TestAddr("a"))  // triggers write exceptions
        .codec(StringCodec)
        .daemon(true) // don't create an exit guard
        .requestTimeout(10.millisecond)
        .hostConnectionLimit(1)
        .hostConnectionMaxWaiters(1)
        .reportTo(mem)
        .build()

    // generate com.twitter.finagle.ChannelWriteException
    intercept[ChannelWriteException] { Await.result(client("hi"), 1.second) }

    val serviceCreationFailures =
      mem.counters(Seq("client", "service_creation", "failures"))
    val requeues =
      mem.counters.get(Seq("client", "retries", "requeues"))

    // initial write exception and no requeues
    assert(serviceCreationFailures == 1)
    assert(requeues == None)
  }

  test("ClientBuilder should be properly instrumented on success") {
    val always = new Service[String, String] {
      def apply(request: String) = Future.value("pong");
    }
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(always)

    val mem = new InMemoryStatsReceiver
    val client = ClientBuilder()
      .name("testClient")
      .hosts(server.boundAddress)
      .codec(StringCodec)
      .hostConnectionLimit(1)
      .hostConnectionMaxWaiters(1)
      .reportTo(mem)
      .retries(1)
      .build()

    Await.result(client("ping"), 10.second)
    Await.ready(server.close(), 1.second)

    val requests = mem.counters(Seq("testClient", "requests"))
    val triesRequests = mem.counters(Seq("testClient", "tries", "requests"))

    assert(requests == 1)
    assert(triesRequests == 1)
  }

  test("ClientBuilderClient.ofCodec should be properly instrumented on success") {
    val always = new Service[String, String] {
      def apply(request: String) = Future.value("pong");
    }
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(always)

    val mem = new InMemoryStatsReceiver
    val client = ClientBuilder.stackClientOfCodec(StringCodec.client)
      .configured(DefaultPool.Param(
        /* low        */ 1,
        /* high       */ 1,
        /* bufferSize */ 0,
        /* idleTime   */ 5.seconds,
        /* maxWaiters */ 1))
      .configured(Stats(mem))
      .configured(Retries.Policy(RetryPolicy.tries(1)))
      .newService(Name.bound(server.boundAddress), "testClient")

    Await.result(client("ping"), 1.second)
    Await.ready(server.close(), 1.second)

    val requests = mem.counters(Seq("testClient", "requests"))
    val triesRequests = mem.counters(Seq("testClient", "tries", "requests"))

    assert(requests == 1)
    assert(triesRequests == 1)
  }
}
