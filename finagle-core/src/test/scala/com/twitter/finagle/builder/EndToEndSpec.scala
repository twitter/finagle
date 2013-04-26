package com.twitter.finagle.builder

import com.twitter.finagle.integration.{DynamicCluster, StringCodec}
import com.twitter.finagle.{Service, TooManyConcurrentRequestsException}
import com.twitter.util.{Await, CountDownLatch, Future, Promise}
import java.net.{InetSocketAddress, SocketAddress}
import org.specs.SpecificationWithJUnit

class EndToEndSpec extends SpecificationWithJUnit {
  "Finagle client" should {
    "handle pending request after a host is deleted from cluster" in {
      val constRes = new Promise[String]
      val arrivalLatch = new CountDownLatch(1)
      val service = new Service[String, String] {
        def apply(request: String) = {
          arrivalLatch.countDown()
          constRes
        }
      }
      val address = new InetSocketAddress(0)
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
      response.isDefined must beFalse
      constRes.setValue("foo")
      Await.result(response) must be_==("foo")
    }


    "queue requests while waiting for cluster to initialize" in {
      val echo = new Service[String, String] {
        def apply(request: String) = Future.value(request)
      }
      val address = new InetSocketAddress(0)
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
        responses(i).isDefined must beFalse
      }

      // more than 5 requests will result in MaxQueuedRequestsExceededException
      val r = client("123")
      r.isDefined must beTrue
      Await.ready(r).poll.get.isThrow must beTrue
      Await.result(r) must throwA(new TooManyConcurrentRequestsException)

      // make cluster available, now queued requests should be processed
      val thread = new Thread {
        override def run = cluster.add(server.localAddress)
      }

      cluster.ready.map { _ =>
        0 until 5 foreach { i =>
          Await.result(responses(i)) must be_==(i.toString)
        }
      }
      thread.start()
      thread.join()
    }
  }
}


