package com.twitter.finagle.loadbalancer

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.StringClient
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.stats.{InMemoryHostStatsReceiver, InMemoryStatsReceiver, StatsReceiver}
import com.twitter.util.{Activity, Await, Future, Time, Var}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

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

    server1.close()
    server2.close()
  }

  test("throws NoBrokersAvailableException with negative addresses") {
    val next: Stack[ServiceFactory[String, String]] =
      Stack.Leaf(Stack.Role("mock"), ServiceFactory.const[String, String](
        Service.mk[String, String](req => Future.value(req))))

    val stack = new LoadBalancerFactory.StackModule[String, String] {
      val description = "mock"
    }.toStack(next)

    val dest = LoadBalancerFactory.Dest(Var(Addr.Neg))
    val factory = stack.make(Stack.Params.empty + dest)
    intercept[NoBrokersAvailableException](Await.result(factory()))
  }

  test("when no nodes are Open and configured to fail fast") {
    val closedSvcFac: ServiceFactory[String, String] = new ServiceFactory[String, String] {
      override def status: Status = Status.Closed
      def apply(clientConnection: ClientConnection): Future[Service[String, String]] = {
        val svc = Service.mk { _: String => Future.value("closed after this") }
        Future.value(svc)
      }
      def close(deadline: Time): Future[Unit] = ???
    }
    val endpoint = Stack.Leaf(Stack.Role("endpoint"), closedSvcFac)
    val stack = LoadBalancerFactory.module.toStack(endpoint)

    val address = Address(InetSocketAddress.createUnresolved("inet-address", 0))
    val factory = stack.make(
      Stack.Params.empty +
        LoadBalancerFactory.Dest(Var(Addr.Bound(address))) +
        LoadBalancerFactory.WhenNoNodesOpenParam(WhenNoNodesOpen.FailFast))

    // as `factory.status == Open` until we have "primed" the pump.
    // services are lazily established and are considered "Open" until that point.
    Await.ready(factory(ClientConnection.nil), 5.seconds)

    // now that the service is primed, we should fail fast.
    assert(factory.status == Status.Closed)
    intercept[NoNodesOpenException] {
      Await.result(factory(ClientConnection.nil), 5.seconds)
    }
  }

  test("when no nodes are Open and not configured to fail fast") {
    val closedSvcFac: ServiceFactory[String, String] = new ServiceFactory[String, String] {
      override def status: Status = Status.Closed
      def apply(clientConnection: ClientConnection): Future[Service[String, String]] = {
        val svc = Service.mk { _: String => Future.value("closed after this") }
        Future.value(svc)
      }
      def close(deadline: Time): Future[Unit] = ???
    }
    val endpoint = Stack.Leaf(Stack.Role("endpoint"), closedSvcFac)
    val stack = LoadBalancerFactory.module.toStack(endpoint)
    val address = Address(InetSocketAddress.createUnresolved("inet-address", 0))
    val factory = stack.make(
      Stack.Params.empty +
        LoadBalancerFactory.Dest(Var(Addr.Bound(address))))

    // as `factory.status == Open` until we have "primed" the pump.
    // services are lazily established and are considered "Open" until that point.
    Await.ready(factory(ClientConnection.nil), 5.seconds)

    // we will not see a failure, even though there are no nodes open
    assert(factory.status == Status.Closed)
    Await.result(factory(ClientConnection.nil), 5.seconds)
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