package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.util.Future
import java.util.concurrent.TimeUnit
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class StatsReceiverTest extends FunSuite {
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
