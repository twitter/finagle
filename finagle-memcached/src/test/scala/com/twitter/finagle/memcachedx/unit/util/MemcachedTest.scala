package com.twitter.finagle.memcachedx.unit

import _root_.java.net.InetSocketAddress
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.memcachedx._
import com.twitter.finagle.param.Stats
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.service._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{Name, Stack, WriteException}
import com.twitter.util.{Time, Await}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class MemcachedTest extends FunSuite with MockitoSugar {
  test("Memcached.Client has expected stack and params") {
    val broker = new Broker[NodeHealth]
    val key = KetamaClientKey("localhost:12345")
    val client =
      Memcached("memcache")
        .configured(FailureAccrualFactory.Param(20, () => 1.seconds))
        .configured(Transporter.ConnectTimeout(100.milliseconds))
        .configured(TimeoutFilter.Param(200.milliseconds))
        .configured(TimeoutFactory.Param(200.milliseconds))
        .configured(param.EjectFailedHost(false))
        .Client(key, broker)

    val stack = client.stack
    assert(stack.contains(Stack.Role("KetamaFailureAccrual")))
    assert(stack.contains(SingletonPool.role))
    
    val params = client.params
    val FailureAccrualFactory.Param.Configured(numFailures, markDeadFor) = params[FailureAccrualFactory.Param]
    assert(numFailures == 20)
    assert(markDeadFor() == 1.seconds)
    assert(params[Transporter.ConnectTimeout] == Transporter.ConnectTimeout(100.milliseconds))
    assert(params[param.EjectFailedHost] == param.EjectFailedHost(false))
    assert(params[FailFastFactory.FailFast] == FailFastFactory.FailFast(false))
  }

  test("Memcache.newClient enables FactoryToService") {
    val st = new InMemoryStatsReceiver
    val client =
      Memcached("memcache")
        .configured(Stats(st))
        .newClient(Name.bound(new InetSocketAddress("127.0.0.1", 12345)))

    val numberRequests = 10
    Time.withCurrentTimeFrozen { _ =>
      for (i <- 0 until numberRequests)
        intercept[WriteException](Await.result(client.get("foo")))
      // Since FactoryToService is enabled, number of requeues should be
      // limited by leaky bucket util it exhausts retries, instead of
      // retrying 25 times on service acquisition
      assert(st.counters(Seq("memcache", "requeue", "budget_exhausted")) == numberRequests)
      // number of requeues = 100 reserved tokens + 20% of qps in the time window
      assert(st.counters(Seq("memcache", "requeue", "requeues")) == numberRequests/5 + 100)
    }
  }
}
