package com.twitter.finagle.builder

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.{StringClient, DefaultPool}
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.integration.StringCodec
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.service.{Retries, RetryPolicy}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.ClientId
import com.twitter.finagle.tracing.Trace
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite with StringClient with StringServer {

  test("IndividualRequestTimeoutException should include RemoteInfo") {
    val timer = new MockTimer
    val reqMade = new Promise[Unit]

    Time.withCurrentTimeFrozen { tc =>
      val svc = new Service[String, String] {
        def apply(request: String) = {
          reqMade.setValue()
          Future.never
        }
      }

      val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      val server = stringServer.serve(address, svc)
      val client = stringClient
        .configured(param.Timer(timer))
        .withSession.acquisitionTimeout(1.seconds)
        .withRequestTimeout(1.seconds)
        .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "B")

      val traceId = Trace.id

      val e = intercept[IndividualRequestTimeoutException] {
        Trace.letId(traceId, true) {
          val res = client("hi")

          // Wait until the request has reached the server so we know it's passed through
          // TimeoutFilter
          Await.result(reqMade, 1.second)
          tc.advance(5.seconds)
          timer.tick()
          Await.result(res, 5.seconds)
        }
      }

      assert(e.remoteInfo ==
        RemoteInfo.Available(None, None, Some(server.boundAddress), Some(ClientId("B")), traceId))
      Await.ready(server.close(), 1.second)
    }
  }

  test("A -> B: Exception returned to A from B should include downstream address of B") {
    val hre = new Service[String, String] {
      def apply(request: String) = Future.exception(new HasRemoteInfo {})
    }

    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = stringServer.serve(address, hre)
    val client = stringClient.newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "B")

    val traceId = Trace.id

    val e = intercept[HasRemoteInfo] {
      Trace.letId(traceId, true) {
        Await.result(client("hi"), 1.second)
      }
    }
    assert(e.remoteInfo == RemoteInfo.Available(None, None, Some(server.boundAddress), Some(ClientId("B")), traceId))
    Await.ready(server.close(), 1.second)
  }

  test("A -> B -> C: Exception returned to B from C should include upstream address of A and downstream address of C") {

    val traceId = Trace.id

    // Make sure this is defined
    val calledC = new Promise[Unit]()

    val serviceC = new Service[String, String] {
      def apply(request: String) = {
        calledC.setDone()
        Future.exception(new HasRemoteInfo {})
      }
    }

    val addressC = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val serverC = stringServer.serve(addressC, serviceC)

    val clientB = stringClient.newService(
      Name.bound(Address(serverC.boundAddress.asInstanceOf[InetSocketAddress])), "C")
    val addressB = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val serviceB = new Service[String, String] {
      def apply(request: String) = {
        val e = intercept[HasRemoteInfo] {
          Trace.letId(traceId, true) {
            Await.result(clientB(request), 1.second)
          }
        }

        // Make sure the remote info upstream addr is pulled from the local context
        assert(e.remoteInfo == RemoteInfo.Available(RemoteInfo.Upstream.addr, Some(ClientId("A")), Some(serverC.boundAddress), Some(ClientId("C")), traceId))

        // The upstream addr isn't available for us to check, but we'll do a sanity check that it's not
        // Server C's address and is actually filled in.
        e.remoteInfo match {
          case RemoteInfo.Available(Some(u), _, _, _, _) =>
            assert(u != serverC.boundAddress)
          case _ => fail("Exception remote info did not have upstream address filled in!")
        }
        Future.exception(e)
      }
    }
    val serverB = stringServer.serve(addressB, serviceB)

    val clientA = stringClient.newService(
      Name.bound(Address(serverB.boundAddress.asInstanceOf[InetSocketAddress])), "B")

    val e = intercept[HasRemoteInfo] {
      Trace.letId(traceId, true) {
        Await.result(clientA("hi"), 3.seconds)
      }
    }

    // Make sure we made it all the way to service C
    Await.result(calledC, 1.second)
    Await.ready(serverC.close(), 1.second)
    Await.ready(serverB.close(), 1.second)

  }

  test("A -> B -> C: Exception returned from B to A should include downstream address of B") {

    val traceId = Trace.id

    val serviceC = new Service[String, String] {
      def apply(request: String) = Future.exception(new HasRemoteInfo {})
    }
    val addressC = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val serverC = stringServer.serve(addressC, serviceC)

    val clientB = stringClient.newService(
      Name.bound(Address(serverC.boundAddress.asInstanceOf[InetSocketAddress])), "C")
    val addressB = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val serviceB = new Service[String, String] {
      def apply(request: String) =
        clientB(request)
    }
    val serverB = stringServer.serveAndAnnounce("B", addressB, serviceB)

    val clientA = stringClient.newService(
      Name.bound(Address(serverB.boundAddress.asInstanceOf[InetSocketAddress])), "B")

    val e = intercept[HasRemoteInfo] {
      Trace.letId(traceId, true) {
        Await.result(clientA("hi"), 1.second)
      }
    }
    assert(e.remoteInfo == RemoteInfo.Available(None, None, Some(serverB.boundAddress), Some(ClientId("B")), traceId))
    Await.ready(serverC.close(), 1.second)
    Await.ready(serverB.close(), 1.second)

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
    val client = {
      val cb = ClientBuilder()
        .name("client")
        .hosts(server.boundAddress.asInstanceOf[InetSocketAddress])
        .codec(StringCodec)
        .daemon(true) // don't create an exit guard
        .requestTimeout(10.millisecond)
        .hostConnectionLimit(1)
        .reportTo(mem)

      val maxWaiters = cb.params[DefaultPool.Param].copy(
        maxWaiters = 1)

      cb.configured(maxWaiters).build()
    }

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
    val client = {
      val cb = ClientBuilder()
        .name("client")
        .addrs(Address.failing)
        .codec(StringCodec)
        .daemon(true) // don't create an exit guard
        .requestTimeout(10.millisecond)
        .hostConnectionLimit(1)
        .reportTo(mem)

      val maxWaiters = cb.params[DefaultPool.Param].copy(
           maxWaiters = 1)
        
      cb.configured(maxWaiters).build()
    }

    // generate com.twitter.finagle.ChannelWriteException
    val traceId = Trace.id

    intercept[IllegalArgumentException] {
      Trace.letId(traceId, true) {
        Await.result(client("hi"), 1.second)
      }
    }

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
      def apply(request: String) = Future.value("pong")
    }
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(always)

    val mem = new InMemoryStatsReceiver
    val client = {
      val cb = ClientBuilder()
        .name("testClient")
        .hosts(server.boundAddress.asInstanceOf[InetSocketAddress])
        .codec(StringCodec)
        .hostConnectionLimit(1)
        .reportTo(mem)
        .retries(1)

      val maxWaiters = cb.params[DefaultPool.Param].copy(
        maxWaiters = 1)

      cb.configured(maxWaiters).build()
    }

    Await.result(client("ping"), 10.second)
    Await.ready(server.close(), 1.second)

    val requests = mem.counters(Seq("testClient", "requests"))
    val triesRequests = mem.counters(Seq("testClient", "tries", "requests"))

    assert(requests == 1)
    assert(triesRequests == 1)

    // need to properly close the client, otherwise it will prevent ExitGuard from exiting and interfere with ExitGuardTest
    Await.ready(client.close(), 1.second)
  }

  test("ClientBuilderClient.ofCodec should be properly instrumented on success") {
    val always = new Service[String, String] {
      def apply(request: String) = Future.value("pong")
    }
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(StringCodec)
      .bindTo(address)
      .name("FinagleServer")
      .build(always)

    val mem = new InMemoryStatsReceiver
    val addr = Address(server.boundAddress.asInstanceOf[InetSocketAddress])
    val client = ClientBuilder.stackClientOfCodec(StringCodec.client)
      .configured(DefaultPool.Param(
        /* low        */ 1,
        /* high       */ 1,
        /* bufferSize */ 0,
        /* idleTime   */ 5.seconds,
        /* maxWaiters */ 1))
      .configured(Stats(mem))
      .configured(Retries.Policy(RetryPolicy.tries(1)))
      .newService(Name.bound(addr), "testClient")

    Await.result(client("ping"), 1.second)
    Await.ready(server.close(), 1.second)

    val requests = mem.counters(Seq("testClient", "requests"))
    val triesRequests = mem.counters(Seq("testClient", "tries", "requests"))

    assert(requests == 1)
    assert(triesRequests == 1)

    // need to properly close the client, otherwise it will prevent ExitGuard from exiting and interfere with ExitGuardTest
    Await.ready(client.close(), 1.second)
  }
}
