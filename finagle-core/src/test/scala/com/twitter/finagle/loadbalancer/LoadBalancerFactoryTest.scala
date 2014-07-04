package com.twitter.finagle.loadbalancer

import com.twitter.finagle.client.StringClient
import com.twitter.finagle.param
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.stats.{InMemoryStatsReceiver, LoadedStatsReceiver, NullStatsReceiver}
import com.twitter.util.{Await, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import java.net.{SocketAddress, InetSocketAddress}
import com.twitter.finagle.Name
import com.twitter.app.App

@RunWith(classOf[JUnitRunner])
class LoadBalancerFactoryTest extends FunSuite with StringClient {

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
      flag.parse(Array("-com.twitter.finagle.loadbalancer.perHostStats=true"))
    def disablePerHostStats() = 
      flag.parse(Array("-com.twitter.finagle.loadbalancer.perHostStats=false"))
    //ensure the per-host stats are disabled if previous test didn't call disablePerHostStats()
    disablePerHostStats()
  }

  test("per-host stats flag not set, no configured per-host stats.\n" + 
    "No per-host stats should be reported") (new PerHostFlagCtx {
    val loadedStatsReceiver = new InMemoryStatsReceiver
    LoadedStatsReceiver.self = loadedStatsReceiver
    client.configured(param.Label(label))
      .newService(port)
    assert(loadedStatsReceiver.gauges.contains(perHostStatKey) === false)
    
    disablePerHostStats()
  })

  test("per-host stats flag not set, configured per-host stats.\n" +
    "Per-host stats should be reported to configured stats receiver") (new PerHostFlagCtx {
    val hostStatsReceiver = new InMemoryStatsReceiver
    client.configured(param.Label(label))
      .configured(LoadBalancerFactory.HostStats(hostStatsReceiver))
      .newService(port)
    assert(hostStatsReceiver.gauges(perHostStatKey).apply === 1.0)

    disablePerHostStats()
  })

  test("per-host stats flag set, no configured per-host stats.\n" + 
    "Per-host stats should be reported to loadedStatsReceiver") (new PerHostFlagCtx {
    enablePerHostStats()

    val hostStatsReceiver = new InMemoryStatsReceiver
    LoadedStatsReceiver.self = hostStatsReceiver
    client.configured(param.Label(label))
      .newService(port)
    assert(hostStatsReceiver.gauges(perHostStatKey).apply === 1.0)

    disablePerHostStats()
  })

  test("per-host stats flag set, configured per-host stats.\n" + 
    "Per-host stats should be reported to configured stats receiver") (new PerHostFlagCtx {
    enablePerHostStats()

    val hostStatsReceiver = new InMemoryStatsReceiver
    client.configured(param.Label(label))
      .configured(LoadBalancerFactory.HostStats(hostStatsReceiver))
      .newService(port)
    assert(hostStatsReceiver.gauges(perHostStatKey).apply === 1.0)

    disablePerHostStats()
  })

  test("per-host stats flag set, configured per-host stats is NullStatsReceiver.\n" +
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
}