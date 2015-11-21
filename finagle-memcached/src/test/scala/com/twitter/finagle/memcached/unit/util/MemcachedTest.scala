package com.twitter.finagle.memcached.unit.util

import com.twitter.conversions.time._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.Memcached
import com.twitter.finagle.param.Stats
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.service._
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.WriteException
import com.twitter.util.{Time, Await}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{IntegrationPatience, Eventually}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class MemcachedTest extends FunSuite
  with MockitoSugar
  with Eventually
  with IntegrationPatience
{
  test("Memcached.Client has expected stack and params") {
    val markDeadFor = Backoff.const(1.second)
    val failureAccrualPolicy = FailureAccrualPolicy.consecutiveFailures(20, markDeadFor)
    val client = Memcached.client
      .configured(FailureAccrualFactory.Param(failureAccrualPolicy))
      .configured(Transporter.ConnectTimeout(100.milliseconds))
      .configured(TimeoutFilter.Param(200.milliseconds))
      .configured(TimeoutFactory.Param(200.milliseconds))
      .configured(Memcached.param.EjectFailedHost(false))

    val stack = client.stack
    assert(stack.contains(FailureAccrualFactory.role))
    assert(stack.contains(SingletonPool.role))

    val params = client.params

    val FailureAccrualFactory.Param.Configured(policy) = params[FailureAccrualFactory.Param]
    assert(policy() == failureAccrualPolicy)
    assert(markDeadFor.take(10).force.toSeq === (0 until 10 map { _ => 1.second }))
    assert(params[Transporter.ConnectTimeout] == Transporter.ConnectTimeout(100.milliseconds))
    assert(params[Memcached.param.EjectFailedHost] == Memcached.param.EjectFailedHost(false))
    assert(params[FailFastFactory.FailFast] == FailFastFactory.FailFast(false))
  }

  test("Memcache.newPartitionedClient enables FactoryToService") {
    val st = new InMemoryStatsReceiver
    val client = Memcached.client
      .configured(Stats(st))
      .newRichClient("memcache=127.0.0.1:12345")

    // wait until we have at least 1 node, or risk getting a ShardNotAvailable exception
    eventually {
      assert(st.gauges(Seq("live_nodes"))() >= 1)
    }

    val numberRequests = 10
    Time.withCurrentTimeFrozen { _ =>
      for (i <- 0 until numberRequests)
        intercept[WriteException](Await.result(client.get("foo"), 3.seconds))
      // Since FactoryToService is enabled, number of requeues should be
      // limited by leaky bucket until it exhausts retries, instead of
      // retrying 25 times on service acquisition.
      // number of requeues = maxRetriesPerReq * numRequests
      assert(st.counters(Seq("memcache", "retries", "requeues")) > numberRequests)
    }
  }
}
