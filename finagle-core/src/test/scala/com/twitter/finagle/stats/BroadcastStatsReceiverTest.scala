package com.twitter.finagle.stats

import com.twitter.util.{Future, Await}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class BroadcastStatsReceiverTest extends FunSuite
  with ShouldMatchers
{

  test("counter") {
    val recv1 = new InMemoryStatsReceiver
    val recvA = recv1.scope("scopeA")
    val recvB = recv1.scope("scopeB")

    val broadcastA = BroadcastStatsReceiver(Seq(recv1, recvA))
    val counterA = broadcastA.counter("hi")
    assert(None === recv1.counters.get(Seq("hi")))
    assert(None === recv1.counters.get(Seq("scopeA", "hi")))

    counterA.incr(1)
    assert(1 === recv1.counters(Seq("hi")))
    assert(1 === recv1.counters(Seq("scopeA", "hi")))

    val broadcastB = BroadcastStatsReceiver(Seq(recv1, recvB))
    val counterB = broadcastB.counter("hi")
    assert(None === recv1.counters.get(Seq("scopeB", "hi")))

    counterB.incr(1)
    assert(2 === recv1.counters(Seq("hi")))
    assert(1 === recv1.counters(Seq("scopeA", "hi")))
    assert(1 === recv1.counters(Seq("scopeB", "hi")))
  }

  test("stat") {
    val recv1 = new InMemoryStatsReceiver
    val recvA = recv1.scope("scopeA")
    val recvB = recv1.scope("scopeB")

    val broadcastA = BroadcastStatsReceiver(Seq(recv1, recvA))
    val statA = broadcastA.stat("hi")
    assert(None === recv1.stats.get(Seq("hi")))
    assert(None === recv1.stats.get(Seq("scopeA", "hi")))

    statA.add(5f)
    assert(Seq(5f) === recv1.stats(Seq("hi")))
    assert(Seq(5f) === recv1.stats(Seq("scopeA", "hi")))

    val broadcastB = BroadcastStatsReceiver(Seq(recv1, recvB))
    val statB = broadcastB.stat("hi")
    assert(None === recv1.stats.get(Seq("scopeB", "hi")))

    statB.add(10f)
    assert(Seq(5f, 10f) === recv1.stats(Seq("hi")).sorted)
    assert(Seq(5f) === recv1.stats(Seq("scopeA", "hi")))
    assert(Seq(10f) === recv1.stats(Seq("scopeB", "hi")))
  }

  test("gauge") {
    val recv1 = new InMemoryStatsReceiver
    val recvA = recv1.scope("scopeA")

    val broadcastA = BroadcastStatsReceiver(Seq(recv1, recvA))
    assert(None === recv1.gauges.get(Seq("hi")))
    assert(None === recv1.gauges.get(Seq("scopeA", "hi")))

    val gaugeA = broadcastA.addGauge("hi") { 5f }
    assert(5f === recv1.gauges(Seq("hi"))())
    assert(5f === recv1.gauges(Seq("scopeA", "hi"))())

    gaugeA.remove()
    assert(None === recv1.gauges.get(Seq("hi")))
    assert(None === recv1.gauges.get(Seq("scopeA", "hi")))
  }

  test("scope") {
    val base = new InMemoryStatsReceiver
    val scoped = base.scope("scoped")
    val subscoped = BroadcastStatsReceiver(Seq(base, scoped)).scope("subscoped")

    val counter = subscoped.counter("yolo")
    counter.incr(9)

    assert(9 === base.counters(Seq("subscoped", "yolo")))
    assert(9 === base.counters(Seq("scoped", "subscoped", "yolo")))
  }

  test("scopeSuffix") {
    val base = new InMemoryStatsReceiver
    val scoped = base.scope("scoped")
    val subscoped = BroadcastStatsReceiver(Seq(base, scoped))
      .scopeSuffix("suffixed")
      .scope("sub")

    val counter = subscoped.counter("yolo")
    counter.incr(9)

    assert(9 === base.counters(Seq("sub", "suffixed", "yolo")))
    assert(9 === base.counters(Seq("scoped", "sub", "suffixed", "yolo")))
  }

  test("time") {
    val recv1 = new InMemoryStatsReceiver
    val recv2 = new InMemoryStatsReceiver
    val recv = BroadcastStatsReceiver(Seq(recv1, recv2))

    val statName = Seq("meh")
    recv1.stats.get(statName).isEmpty should be(true)
    recv2.stats.get(statName).isEmpty should be(true)

    recv.time("meh")()
    recv1.stats(statName).size should be(1)
    recv2.stats(statName).size should be(1)

    recv.time("meh")()
    recv1.stats(statName).size should be(2)
    recv2.stats(statName).size should be(2)
  }

  test("timeFuture") {
    val recv1 = new InMemoryStatsReceiver
    val recv2 = new InMemoryStatsReceiver
    val recv = BroadcastStatsReceiver(Seq(recv1, recv2))

    val statName = Seq("meh")
    recv1.stats.get(statName).isEmpty should be(true)
    recv2.stats.get(statName).isEmpty should be(true)

    Await.result(recv.timeFuture("meh")(Future.Unit))
    recv1.stats(statName).size should be(1)
    recv2.stats(statName).size should be(1)

    Await.result(recv.timeFuture("meh")(Future.Unit))
    recv1.stats(statName).size should be(2)
    recv2.stats(statName).size should be(2)
  }

}
