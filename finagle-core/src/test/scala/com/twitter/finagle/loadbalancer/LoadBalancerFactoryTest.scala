package com.twitter.finagle.loadbalancer

import com.twitter.app.App
import com.twitter.finagle._
import com.twitter.finagle.client.StringClient
import com.twitter.util.{Await, Future, Time}
import com.twitter.finagle.{NoBrokersAvailableException, param}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, InMemoryHostStatsReceiver,
  LoadedStatsReceiver, LoadedHostStatsReceiver, NullStatsReceiver}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class LoadBalancerFactoryTest extends FunSuite
  with StringClient
  with Eventually
  with IntegrationPatience
{
  import LoadBalancerFactory.WeightedFactory

  trait Ctx {
    val sr = new InMemoryStatsReceiver
    val client = stringClient
      .configured(param.Stats(sr))
  }

  trait PerHostFlagCtx extends Ctx with App {
    val label = "myclient"
    val port = "localhost:8080"
    val perHostStatKey = Seq(label, port, "available")

    def enablePerHostStats() =
      flag.parseArgs(Array("-com.twitter.finagle.loadbalancer.perHostStats=true"))
    def disablePerHostStats() =
      flag.parseArgs(Array("-com.twitter.finagle.loadbalancer.perHostStats=false"))
    //ensure the per-host stats are disabled if previous test didn't call disablePerHostStats()
    disablePerHostStats()
  }

  case class MockServiceFactory(arg: String) extends ServiceFactory[Unit, Unit] {
    def apply(conn: ClientConnection) = Future.value(Service.mk(_ => Future.Done))
    def close(deadline: Time) = Future.Done
  }

  def mkFactory(sa: SocketAddress): WeightedFactory[Unit, Unit] = sa match {
    case TestAddr(arg) => (MockServiceFactory(arg), 1)
    case sa => (MockServiceFactory(sa.toString), 1)
  }

  test("per-host stats flag not set, no configured per-host stats. " +
    "No per-host stats should be reported") (new PerHostFlagCtx {
    val loadedStatsReceiver = new InMemoryStatsReceiver
    LoadedStatsReceiver.self = loadedStatsReceiver
    client.configured(param.Label(label))
      .newService(port)
    assert(loadedStatsReceiver.gauges.contains(perHostStatKey) === false)

    disablePerHostStats()
  })

  test("per-host stats flag not set, configured per-host stats. " +
    "Per-host stats should be reported to configured stats receiver") (new PerHostFlagCtx {
    val hostStatsReceiver = new InMemoryStatsReceiver
    client.configured(param.Label(label))
      .configured(LoadBalancerFactory.HostStats(hostStatsReceiver))
      .newService(port)
    eventually {
      assert(hostStatsReceiver.gauges(perHostStatKey).apply === 1.0)
    }
    disablePerHostStats()
  })

  test("per-host stats flag set, no configured per-host stats. " +
    "Per-host stats should be reported to loadedHostStatsReceiver") (new PerHostFlagCtx {
    enablePerHostStats()

    val hostStatsReceiver = new InMemoryStatsReceiver
    LoadedHostStatsReceiver._self = hostStatsReceiver
    client.configured(param.Label(label))
      .newService(port)
    eventually {
      assert(hostStatsReceiver.gauges(perHostStatKey).apply === 1.0)
    }
    disablePerHostStats()
  })

  test("per-host stats flag set, configured per-host stats. " +
    "Per-host stats should be reported to configured stats receiver") (new PerHostFlagCtx {
    enablePerHostStats()

    val hostStatsReceiver = new InMemoryStatsReceiver
    client.configured(param.Label(label))
      .configured(LoadBalancerFactory.HostStats(hostStatsReceiver))
      .newService(port)
    eventually {
      assert(hostStatsReceiver.gauges(perHostStatKey).apply === 1.0)
    }
    disablePerHostStats()
  })

  test("per-host stats flag set, configured per-host stats is NullStatsReceiver. " +
    "Per-host stats should not be reported") (new PerHostFlagCtx {
    enablePerHostStats()

    val loadedStatsReceiver = new InMemoryStatsReceiver
    LoadedStatsReceiver.self = loadedStatsReceiver
    client.configured(param.Label(label))
      .configured(LoadBalancerFactory.HostStats(NullStatsReceiver))
      .newService(port)
    assert(loadedStatsReceiver.gauges.contains(perHostStatKey) === false)

    disablePerHostStats()
  })

  test("per-host stats flag set, configured per-host stats is a HostStatsReceiver instance. " +
    "Per-host stats should be reported to the hostStatsReceiver") (new PerHostFlagCtx {
    enablePerHostStats()

    val hostStatsReceiver = new InMemoryHostStatsReceiver
    client.configured(param.Label(label))
      .configured(LoadBalancerFactory.HostStats(hostStatsReceiver))
      .newService(port)
    eventually {
      assert(hostStatsReceiver.self.gauges(perHostStatKey).apply === 1.0)
    }
    disablePerHostStats()
  })

  test("per-host stats flag not set, configured per-host stats is a HostStatsReceiver instance. " +
    "Per-host stats should not be reported") (new PerHostFlagCtx {
    val hostStatsReceiver = new InMemoryHostStatsReceiver
    client.configured(param.Label(label))
      .configured(LoadBalancerFactory.HostStats(hostStatsReceiver))
      .newService(port)
    assert(hostStatsReceiver.self.gauges.contains(perHostStatKey) === false)

    disablePerHostStats()
  })

  test("per-host stats flag not set, configured per-host stats is LoadedHostStatsReceiver. " +
    "Per-host stats should be reported to LoadedHostStatsReceiver") (new PerHostFlagCtx {
    enablePerHostStats()

    val hostStatsReceiver = new InMemoryHostStatsReceiver
    LoadedHostStatsReceiver._self = hostStatsReceiver
    client.configured(param.Label(label))
      .configured(LoadBalancerFactory.HostStats(LoadedHostStatsReceiver))
      .newService(port)
    eventually {
      assert(hostStatsReceiver.self.gauges(perHostStatKey).apply === 1.0)
    }
    disablePerHostStats()
  })

  test("destination name is passed to NoBrokersAvailableException") {
    val name = "nil!"
    val exc = intercept[NoBrokersAvailableException] {
      Await.result(stringClient.newClient(name)())
    }
    assert(exc.name === name)
  }

  test("LoadBalancerFactory#updateFactories: creates factories") {
    val args = Set("a", "b", "c")
    val facs = mutable.Map.empty[SocketAddress, WeightedFactory[Unit, Unit]]

    LoadBalancerFactory.updateFactories[Unit, Unit](
      args.map(TestAddr(_)),
      facs,
      mkFactory,
      probationEnabled = false
    )

    assert(args forall { c =>
      facs exists {
        case (_, (MockServiceFactory(arg), _)) => arg == c
        case _ => false
      }
    })
  }

  test("LoadBalancerFactory#updateFactories: reuse cached factories") {
    val addr = TestAddr("a")
    val fac = mkFactory(addr)
    val facs = mutable.Map[SocketAddress, WeightedFactory[Unit, Unit]](addr -> fac)

    LoadBalancerFactory.updateFactories[Unit, Unit](
      facs.keySet.toSet,
      facs,
      mkFactory,
      probationEnabled = false
    )

    assert(facs(addr) === fac)
  }

  test("LoadBalancerFactory#updateFactories: remove hosts immediately when probation not enabled") {
    val pairs = Set("a", "b", "c") map { c =>
      val addr = TestAddr(c)
      addr -> mkFactory(addr)
    } toSeq

    val facs = mutable.Map[SocketAddress, WeightedFactory[Unit, Unit]](pairs:_*)
    val TestAddr(victimArg) = facs.keySet.head

    // Ensure that lost hosts aren't removed from output.
    LoadBalancerFactory.updateFactories[Unit, Unit](
      facs.keySet.drop(1).toSet,
      facs,
      mkFactory,
      probationEnabled = false
    )

    assert(facs.size == 2)
    assert(facs forall { case (_, (sf, _)) => sf.isAvailable })
  }

  test("LoadBalancerFactory#updateFactories: don't remove available hosts on probation that disappear from input") {
    val pairs = Set("a", "b", "c") map { c =>
      val addr = TestAddr(c)
      addr -> mkFactory(addr)
    } toSeq

    val facs = mutable.Map[SocketAddress, WeightedFactory[Unit, Unit]](pairs:_*)
    val TestAddr(victimArg) = facs.keySet.head

    // Ensure that lost hosts aren't removed from output.
    LoadBalancerFactory.updateFactories[Unit, Unit](
      facs.keySet.drop(1).toSet,
      facs,
      mkFactory,
      probationEnabled = true
    )

    assert(facs.size == 3)
    assert(facs forall { case (_, (sf, _)) => sf.status == Status.Open })
  }

  test("LoadBalancerFactory#updateFactories: remove closed hosts") {
    val closedFactory = new MockServiceFactory("a") {
      override def status = Status.Closed
    }

    val pairs = Set("a", "b", "c") map {
      case "a" => TestAddr("a") -> (closedFactory, 1D)
      case c =>
        val addr = TestAddr(c)
        addr -> mkFactory(addr)
    } toSeq

    val facs = mutable.Map[SocketAddress, WeightedFactory[Unit, Unit]](pairs:_*)

    // Ensure that lost hosts are removed.
    LoadBalancerFactory.updateFactories[Unit, Unit](
      facs.keySet.drop(1).toSet,
      facs,
      mkFactory,
      probationEnabled = false
    )

    assert(facs.size == 2)
  }
}
