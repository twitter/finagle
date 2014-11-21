package com.twitter.finagle.builder

import com.twitter.conversions.time._
import com.twitter.finagle.integration.{DynamicCluster, StringCodec}
import com.twitter.finagle.{Service, WriteException, IndividualRequestTimeoutException}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Future, Await, CountDownLatch, Promise}
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
    val cluster = new DynamicCluster[SocketAddress](Seq(server.localAddress))
    val client = ClientBuilder()
      .cluster(cluster)
      .codec(StringCodec)
      .hostConnectionLimit(1)
      .build()

    // create a pending request; delete the server from cluster
    //  then verify the request can still finish
    val response = client("123")
    arrivalLatch.await()
    cluster.del(server.localAddress)
    assert(!response.isDefined)
    constRes.setValue("foo")
    assert(Await.result(response) === "foo")
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
      override def run = cluster.add(server.localAddress)
    }

    cluster.ready.map { _ =>
      0 until 5 foreach { i =>
        assert(Await.result(responses(i)) === i.toString)
      }
    }
    thread.start()
    thread.join()
  }

  test("ClientBuilder should be properly instrumented") {
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
      .hosts(server.localAddress)
      .codec(StringCodec)
      .requestTimeout(10.millisecond)
      .hostConnectionLimit(1)
      .hostConnectionMaxWaiters(1)
      .reportTo(mem)
      .build()

    // generate com.twitter.finagle.IndividualRequestTimeoutException
    intercept[IndividualRequestTimeoutException] { Await.result(client("hi")) }
    Await.ready(server.close())
    // generate com.twitter.finagle.WriteException$$anon$1
    intercept[WriteException] { Await.result(client("hi")) }

    val requestFailures = mem.counters(Seq("client", "failures"))
    val serviceCreationFailures =
      mem.counters(Seq("client", "service_creation", "failures"))

    assert(requestFailures === 1)
    assert(serviceCreationFailures === 1)
  }
}
