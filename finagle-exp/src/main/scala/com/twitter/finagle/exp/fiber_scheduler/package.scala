package com.twitter.finagle.exp

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.stats.BroadcastStatsReceiver
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.stats.InMemoryStatsReceiver

package object fiber_scheduler {

  // Dumps stats to the stdout if `true`
  // used only for local debugging
  private val dumpStats = false

  private[fiber_scheduler] final val statsReceiver = {
    val default = DefaultStatsReceiver.scope("fiber")
    if (dumpStats) {
      val inMemory = new InMemoryStatsReceiver
      Executors
        .newScheduledThreadPool(
          1,
          new NamedPoolThreadFactory("fiber/dumpStats", makeDaemons = true))
        .scheduleAtFixedRate(() => inMemory.print(System.out), 1, 1, TimeUnit.SECONDS)
      BroadcastStatsReceiver(List(default, inMemory))
    } else {
      default
    }
  }

  private[fiber_scheduler] final val configStatsReceiver = statsReceiver.scope("config")
}
