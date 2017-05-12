package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.client.StringClient
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.{InMemoryHostStatsReceiver, InMemoryStatsReceiver}
import com.twitter.util.{Activity, Await, Future, Var}
import java.net.{InetAddress, InetSocketAddress}
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
        .newService(Name.bound(Address(server1.boundAddress.asInstanceOf[InetSocketAddress]), Address(server2.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 2)
    assert(Await.result(client("hello\n")) == "hello")
  }

  test("throws NoBrokersAvailableException with negative addresses") {
    val next: Stack[ServiceFactory[String, String]] =
      Stack.Leaf(Stack.Role("mock"), ServiceFactory.const[String, String](
        Service.mk[String, String](req => Future.value(req))))

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

  test("Respects the AddressOrdering") {
    val endpoint: Stack[ServiceFactory[String, String]] =
      Stack.Leaf(Stack.Role("endpoint"), ServiceFactory.const[String, String](
        Service.mk[String, String](req => ???)))

    val stack = LoadBalancerFactory.module[String, String].toStack(endpoint)

    var eps: Vector[String] = Vector.empty
    val mockBalancer = new LoadBalancerFactory {
      def newBalancer[Req, Rep](
        endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]],
        statsReceiver: StatsReceiver,
        emptyException: NoBrokersAvailableException
      ): ServiceFactory[Req, Rep] = {
        // this relies on the toString of the ServiceFactory
        // inside the LoadBalancerFactory, not the best way
        // to get at the underlying addr, but not sure there
        // is another way since we can't change the type of
        // Stack modules and Stack is invariant.
        eps = endpoints.sample().toVector.map(_.toString)
        ServiceFactory.const(Service.mk(_ => ???))
      }
    }

    val addresses = (10 to 0 by -1).map { i =>
      Address(InetSocketAddress.createUnresolved(s"inet-address-$i", 0))
    }

    var orderCalled = false
    val order: Ordering[Address] = new Ordering[Address] {
      def compare(a0: Address, a1: Address): Int = {
        orderCalled = true
        a0.toString.compare(a1.toString)
      }
    }

    stack.make(Stack.Params.empty +
      LoadBalancerFactory.Param(mockBalancer) +
      LoadBalancerFactory.Dest(Var(Addr.Bound(addresses.toSet))) +
      LoadBalancerFactory.AddressOrdering(order))

    assert(orderCalled)
    val sortedAddresses: Seq[String] = addresses.sortBy(_.toString).map(_.toString)
    eps.indices.foreach { i => assert(eps(i) == sortedAddresses(i)) }
  }
}