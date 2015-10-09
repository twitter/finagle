package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.client.{StackClient, StringClient}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.stats.{InMemoryHostStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util.{Await, Future, Var}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LoadBalancerFactoryTest extends FunSuite
  with StringClient
  with StringServer
  with Eventually
  with IntegrationPatience {
  val echoService = Service.mk[String, String](Future.value(_))

  trait PerHostFlagCtx extends App {
    val label = "myclient"
    val client = stringClient.configured(param.Label(label))
    val port = "localhost:8080"
    val perHostStatKey = Seq(label, port, "available")
  }

  test("reports per-host stats when flag is true") {
    new PerHostFlagCtx {
      val sr = new InMemoryHostStatsReceiver
      val sr1 = new InMemoryStatsReceiver

      perHostStats.let(true) {
        client.configured(LoadBalancerFactory.HostStats(sr))
          .newService(port)
        eventually {
          assert(sr.self.gauges(perHostStatKey).apply == 1.0)
        }

        client.configured(LoadBalancerFactory.HostStats(sr1))
          .newService(port)
        eventually {
          assert(sr1.gauges(perHostStatKey).apply == 1.0)
        }
      }
    }
  }

  test("does not report per-host stats when flag is false") {
    new PerHostFlagCtx {
      val sr = new InMemoryHostStatsReceiver
      val sr1 = new InMemoryStatsReceiver

      perHostStats.let(false) {
        client.configured(LoadBalancerFactory.HostStats(sr))
          .newService(port)
        assert(sr.self.gauges.contains(perHostStatKey) == false)

        client.configured(LoadBalancerFactory.HostStats(sr1))
          .newService(port)
        assert(sr1.gauges.contains(perHostStatKey) == false)
      }
    }
  }

  test("make service factory stack") {
    val addr1 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server1 = stringServer.serve(addr1, echoService)

    val addr2 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server2 = stringServer.serve(addr2, echoService)

    val sr = new InMemoryStatsReceiver
    val client = stringClient
        .configured(Stats(sr))
        .newService(Name.bound(server1.boundAddress, server2.boundAddress), "client")

    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 2)
    assert(Await.result(client("hello\n")) == "hello")
  }

  test("throws NoBrokersAvailableException with negative addresses") {
    val next: Stack[ServiceFactory[String, String]] =
      Stack.Leaf(Stack.Role("mock"), ServiceFactory.const[String, String](
        Service.mk[String, String](req => Future.value(s"$req"))))

    val stack = new LoadBalancerFactory.StackModule[String, String] {
      val description = "mock"
    }.toStack(next)

    val addrs = Seq(Addr.Neg)
    addrs.foreach { addr =>
      val dest = LoadBalancerFactory.Dest(Var(addr))
      val factory = stack.make(Stack.Params.empty + dest)
      intercept[NoBrokersAvailableException](Await.result(factory()))
    }
  }
}

@RunWith(classOf[JUnitRunner])
class SocketAddressesTest extends FunSuite {
  import SocketAddresses._

  val sa = new SocketAddress {}
  val wsa = WeightedSocketAddress(sa, 2.0)
  def newWrapped: SocketAddress = new SocketAddresses.Wrapped {
    val underlying = sa
  }

  def replicator(sa: SocketAddress): Set[SocketAddress] =
    for (i: Int <- (0 until 2).toSet) yield
        WeightedSocketAddress(newWrapped, 1.0)

  test("unwraps address") {
    assert(SocketAddresses.unwrap(sa) == sa)
    assert(SocketAddresses.unwrap(wsa) == sa)
    replicator(wsa).foreach { addr =>
      assert(SocketAddresses.unwrap(addr) == sa)
    }
    replicator(wsa).foreach { replica => // Weighted(replicated(sa), w)
      (SocketAddresses.unwrap(WeightedSocketAddress(replica, 2.0)) == sa)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class ConcurrentLoadBalancerFactoryTest extends FunSuite with StringClient with StringServer {
  val echoService = Service.mk[String, String](Future.value(_))

  test("makes service factory stack") {
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = stringServer.serve(address, echoService)

    val sr = new InMemoryStatsReceiver
    val clientStack =
      StackClient.newStack.replace(
        LoadBalancerFactory.role, ConcurrentLoadBalancerFactory.module[String, String])
    val client = stringClient.withStack(clientStack)
      .configured(Stats(sr))
      .newService(Name.bound(server.boundAddress), "client")

    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 4)
    assert(Await.result(client("hello\n")) == "hello")
  }

  test("creates fixed number of service factories based on params") {
    val addr1 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server1 = stringServer.serve(addr1, echoService)

    val addr2 = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server2 = stringServer.serve(addr2, echoService)

    val sr = new InMemoryStatsReceiver
    val clientStack =
      StackClient.newStack.replace(
        LoadBalancerFactory.role, ConcurrentLoadBalancerFactory.module[String, String])
    val client = stringClient.withStack(clientStack)
      .configured(Stats(sr))
      .configured(ConcurrentLoadBalancerFactory.Param(3))
      .newService(Name.bound(server1.boundAddress, server2.boundAddress), "client")

    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 6)
  }
}