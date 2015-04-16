package com.twitter.finagle.memcachedx.unit

import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.memcachedx._
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.service._
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
    val FailureAccrualFactory.Param(numFailures, markDeadFor) = params[FailureAccrualFactory.Param]
    assert(numFailures == 20)
    assert(markDeadFor() == 1.seconds)
    assert(params[Transporter.ConnectTimeout] == Transporter.ConnectTimeout(100.milliseconds))
    assert(params[param.EjectFailedHost] == param.EjectFailedHost(false))
    assert(params[FailFastFactory.FailFast] == FailFastFactory.FailFast(false))
  }
}