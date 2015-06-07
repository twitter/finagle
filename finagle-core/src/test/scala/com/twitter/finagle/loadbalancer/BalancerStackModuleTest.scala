package com.twitter.finagle.loadbalancer

import com.twitter.app.App
import com.twitter.finagle.client.StringClient
import com.twitter.finagle._
import com.twitter.finagle.stats.{InMemoryHostStatsReceiver, InMemoryStatsReceiver}
import com.twitter.util.{Var, Time, Future, Await}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BalancerStackModuleTest extends FunSuite with StringClient {
  type WeightedFactory[Req, Rep] = (ServiceFactory[Req, Rep], Double)

  trait PerHostFlagCtx extends App {
    val rng = new Random
    val label = "myclient"
    val client = stringClient.configured(param.Label(label))
    val port = "localhost:8080"
    val perHostStatKey = Seq(label, port, "available")
  }

  class MockServiceFactory(arg: String) extends ServiceFactory[String, String] {
    val identifier: String = arg
    def apply(conn: ClientConnection) =
      Future.value(Service.mk[String, String](req => Future.value(s"$arg: $req")))
    def close(deadline: Time) = Future.Done
    override def status: Status = Status.Open
  }

  def mkFactory(sa: SocketAddress): ServiceFactory[String, String] = {
    SocketAddresses.unwrap(sa) match {
      case TestAddr(arg) => new MockServiceFactory(arg)
      case sa => new MockServiceFactory(sa.toString)
    }
  }

  object BalancerModule extends BalancerStackModule[String, String] {
    val parameters = Seq()
    val description = "test"
  }

  test("reports per-host stats when flag is true") {
    new PerHostFlagCtx {
      val sr = new InMemoryHostStatsReceiver
      val sr1 = new InMemoryStatsReceiver

      perHostStats.let(true) {
        client.configured(LoadBalancerFactory.HostStats(sr))
          .newService(port)
        assert(sr.self.gauges(perHostStatKey).apply == 1.0)

        client.configured(LoadBalancerFactory.HostStats(sr1))
          .newService(port)
        assert(sr1.gauges(perHostStatKey).apply == 1.0)
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

  test("updateFactories: creates and caches factories") {
    val pairs = Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0)
    val addrs: Set[SocketAddress] = pairs.map { case (name, weight) =>
      WeightedSocketAddress(TestAddr(name), weight)
    }.toSet

    val facs = mutable.Map.empty[SocketAddress, WeightedFactory[String, String]]

    BalancerModule.updateFactories[String, String](
      addrs,
      facs,
      mkFactory,
      probationEnabled = false
    )

    // create one service factory for each address, and cache them
    assert(facs.size == 3)
    withClue("one factory for each address \"a\", \"b\" and \"c\": ") {
      assert(pairs.forall { case (name, weight) =>
        val (f, w) = facs(TestAddr(name))
        f.asInstanceOf[MockServiceFactory].identifier == name && w == weight
      })
    }
  }

  test("updateFactories: use default weight when not provided") {
    val names = Set("a", "b", "c")
    val facs = mutable.Map.empty[SocketAddress, WeightedFactory[String, String]]

    BalancerModule.updateFactories[String, String](
      names.map(TestAddr(_)),
      facs,
      mkFactory,
      probationEnabled = false
    )

    // create new service factories and cache them
    assert(facs.size == 3)
    withClue("factory of \"a\", \"b\" and \"c\" has default weight 1.0: ") {
      assert(names.forall { name =>
        val (f, w) = facs(TestAddr(name))
        f.asInstanceOf[MockServiceFactory].identifier == name && w == 1.0 // weights default to 1.0
      })
    }
  }

  test("updateFactories: reuse cached factories") {
    val addr = TestAddr("a")
    val fac = mkFactory(addr)
    val facs = mutable.Map[SocketAddress, WeightedFactory[String, String]](addr -> (fac, 1.0))

    BalancerModule.updateFactories[String, String](
      Set(addr),
      facs,
      mkFactory,
      probationEnabled = false
    )

    // cached factories unchanged
    assert(facs.size == 1)
    assert(facs(addr) == (fac, 1.0))
  }

  test("updateFactories: reuse cached factories with updated weights") {
    val unweightedAddr = TestAddr("a")
    val addr = WeightedSocketAddress(unweightedAddr, 1.0)
    val fac = mkFactory(addr)
    val facs = mutable.Map[SocketAddress, WeightedFactory[String, String]](unweightedAddr -> (fac, 1.0))

    val anotherAddr = WeightedSocketAddress(unweightedAddr, 2.0)
    BalancerModule.updateFactories[String, String](
      Set(anotherAddr),
      facs,
      mkFactory,
      probationEnabled = false
    )

    // update cached factories with new weight
    assert(facs.size == 1)
    assert(facs(unweightedAddr) == (fac, 2.0))
  }

  test("updateFactories: remove hosts immediately with probation off") {
    val pairs = Set("a", "b", "c").map { c =>
      val addr = TestAddr(c)
      addr -> (mkFactory(addr), 1.0)
    }.toSeq
    val facs = mutable.Map[SocketAddress, WeightedFactory[String, String]](pairs:_*)

    BalancerModule.updateFactories[String, String](
      Set("a", "b").map(c => WeightedSocketAddress(TestAddr(c), 2.0)),
      facs,
      mkFactory,
      probationEnabled = false
    )

    // "c" is remove
    assert(facs.size == 2)
    withClue("factory of \"c\" should be removed: ") {
      assert(Set("a", "b").forall { name =>
        val (f, w) = facs(TestAddr(name))
        f.asInstanceOf[MockServiceFactory].identifier == name && w == 2.0 && f.isAvailable
      })
    }
  }

  test("updateFactories: don't remove available hosts that disappear from input with probation on") {
    val pairs = Set("a", "b", "c").map { name =>
      val addr = TestAddr(name)
      addr -> (mkFactory(addr), 1.0)
    }.toSeq

    val facs = mutable.Map[SocketAddress, WeightedFactory[String, String]](pairs:_*)

    BalancerModule.updateFactories[String, String](
      Set("a", "b").map(n => WeightedSocketAddress(TestAddr(n), 2.0)),
      facs,
      mkFactory,
      probationEnabled = true
    )

    // "c" remains in cache
    assert(facs.size == 3)
    withClue("factory of \"c\" should remain in the set: ") {
      assert(Set("a", "b", "c").forall { name =>
        val (f, w) = facs(TestAddr(name))
        val expectedWeight = if (name == "c") 1.0 else 2.0
        f.asInstanceOf[MockServiceFactory].identifier == name && w == expectedWeight && f.isAvailable
      })
    }
  }

  test("updateFactories: remove closed hosts") {
    val closedFactory = new MockServiceFactory("a") {
      override def status: Status = Status.Closed
    }

    val pairs = Set("a", "b", "c").map {
      case "c" =>
        TestAddr("c") -> (closedFactory, 1.0)
      case n   =>
        val addr = TestAddr(n)
        addr -> (mkFactory(addr), 1.0)
    }.toSeq
    val facs = mutable.Map[SocketAddress, WeightedFactory[String, String]](pairs:_*)

    BalancerModule.updateFactories[String, String](
      Set("a", "b").map(n => WeightedSocketAddress(TestAddr(n), 2.0)),
      facs,
      mkFactory,
      probationEnabled = true
    )

    // "c" removed from cache
    assert(facs.size == 2)
    withClue("closed factory of \"c\" should be removed: ") {
      assert(Set("a", "b").forall { name =>
        val (f, w) = facs(TestAddr(name))
        f.asInstanceOf[MockServiceFactory].identifier == name && w == 2.0
      })
    }
  }

  test("updateFactories: reuse factories cached by ReplicatedSocketAddress") {
    val baseAddr = TestAddr("a")
    val replicas = SocketAddresses.replicate(2)(baseAddr)
    val snapshot = replicas.map { addr =>
      val (base, _) = WeightedSocketAddress.extract(addr)
      base -> (mkFactory(addr), 1.0)
    }.toMap
    val facs = mutable.Map(snapshot.toSeq: _*)

    BalancerModule.updateFactories[String, String](
      replicas,
      facs,
      mkFactory,
      probationEnabled = false
    )

    // cached factories unchanged
    assert(facs == snapshot)
  }

  test("makes service factory stack") {
    val next: Stack[ServiceFactory[String, String]] =
      Stack.Leaf(Stack.Role("mock"), new MockServiceFactory("mock"))
    val stack = BalancerModule.toStack(next)

    val dest = LoadBalancerFactory.Dest(Var(Addr.Bound(TestAddr("a"))))
    val factory = stack.make(Stack.Params.empty + dest)

    assert(Await.result(Await.result(factory())("test")) == "mock: test")
  }

  test("throws NoBrokersAvailableException with negative addresses") {
    val next: Stack[ServiceFactory[String, String]] =
      Stack.Leaf(Stack.Role("mock"), new MockServiceFactory("mock"))
    val stack = BalancerModule.toStack(next)

    val addrs = Seq(Addr.Failed(new Exception), Addr.Neg)
    addrs.foreach { addr =>
      val dest = LoadBalancerFactory.Dest(Var(addr))
      val factory = stack.make(Stack.Params.empty + dest)
      intercept[NoBrokersAvailableException](Await.result(factory()))
    }
  }
}