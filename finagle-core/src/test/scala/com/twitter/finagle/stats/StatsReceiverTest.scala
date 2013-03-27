package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.util.Future
import java.util.concurrent.TimeUnit
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class StatsReceiverTest extends FunSuite {
  test("RollupStatsReceiver counter/stats") {
    val mem = new InMemoryStatsReceiver
    val receiver = new RollupStatsReceiver(mem)

    receiver.counter("toto", "titi", "tata").incr()
    assert(mem.counters(Seq("toto")) == 1)
    assert(mem.counters(Seq("toto", "titi")) == 1)
    assert(mem.counters(Seq("toto", "titi", "tata")) == 1)

    receiver.counter("toto", "titi", "tutu").incr()
    assert(mem.counters(Seq("toto")) == 2)
    assert(mem.counters(Seq("toto", "titi")) == 2)
    assert(mem.counters(Seq("toto", "titi", "tata")) == 1)
    assert(mem.counters(Seq("toto", "titi", "tutu")) == 1)
  }

  test("Broadcast Counter/Stat") {
    class MemCounter extends Counter {
      var c = 0
      def incr(delta: Int) { c += delta }
    }
    val c1 = new MemCounter
    val c2 = new MemCounter
    val broadcastCounter = BroadcastCounter(Seq(c1, c2))
    assert(c1.c == 0)
    assert(c2.c == 0)

    broadcastCounter.incr()
    assert(c1.c == 1)
    assert(c2.c == 1)

    class MemStat extends Stat {
      var values: Seq[Float] = ArrayBuffer.empty[Float]
      def add(f: Float) { values = values :+ f }
    }
    val s1 = new MemStat
    val s2 = new MemStat
    val broadcastStat = BroadcastStat(Seq(s1, s2))
    assert(s1.values === Seq.empty)
    assert(s2.values === Seq.empty)

    broadcastStat.add(1F)
    assert(s1.values === Seq(1F))
    assert(s2.values === Seq(1F))
  }

  test("StatsReceiver time") {
    val receiver = spy(new InMemoryStatsReceiver)

    receiver.time("er", "mah", "gerd") { () }
    verify(receiver, times(1)).stat("er", "mah", "gerd")

    receiver.time(TimeUnit.NANOSECONDS, "er", "mah", "gerd") { () }
    verify(receiver, times(2)).stat("er", "mah", "gerd")

    val stat = receiver.stat("er", "mah", "gerd")
    verify(receiver, times(3)).stat("er", "mah", "gerd")

    receiver.time(TimeUnit.DAYS, stat) { () }
    verify(receiver, times(3)).stat("er", "mah", "gerd")
  }

  test("StatsReceiver timeFuture") {
    val receiver = spy(new InMemoryStatsReceiver)

    (receiver.timeFuture("2", "chainz") { Future.Unit })(1.second)
    verify(receiver, times(1)).stat("2", "chainz")

    (receiver.timeFuture(TimeUnit.MINUTES, "2", "chainz") { Future.Unit })(1.second)
    verify(receiver, times(2)).stat("2", "chainz")

    val stat = receiver.stat("2", "chainz")
    verify(receiver, times(3)).stat("2", "chainz")

    (receiver.timeFuture(TimeUnit.HOURS, stat) { Future.Unit })(1.second)
    verify(receiver, times(3)).stat("2", "chainz")
  }

  test("StatsReceiver equality with NullStatsReceiver") {
    val previous = LoadedStatsReceiver.self
    LoadedStatsReceiver.self = NullStatsReceiver

    try{
      val statsReceiver = DefaultStatsReceiver
      assert(statsReceiver == NullStatsReceiver,
        "Can't detect that statsReceiver is NullStatsReceiver")

      val mem = new InMemoryStatsReceiver
      LoadedStatsReceiver.self = mem
      assert(statsReceiver != NullStatsReceiver,
        "Can't detect that statsReceiver isn't NullStatsReceiver anymore")
      assert(statsReceiver == mem,
        "Can't detect that statsReceiver is InMemoryStatsReceiver")
    } finally {
      // Restore initial state
      LoadedStatsReceiver.self = previous
    }
  }
}
