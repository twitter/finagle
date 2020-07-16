package com.twitter.finagle.loadbalancer.aperture

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.util.{Future, Time}

private[aperture] object ApertureEagerConnections {
  // exposed for testing
  val MaxConcurrentConnections = 2
  val semaphore = new AsyncSemaphore(MaxConcurrentConnections)

  private val statsReceiver = FinagleStatsReceiver.scope("aperture_eager_connections")
  private val waiters = statsReceiver.addGauge("waiters") { semaphore.numWaiters }
  private val maxConcurrent = statsReceiver.addGauge("max_concurrent") { MaxConcurrentConnections }
  private val latencyStat = statsReceiver.stat("permit_latency_ms")

  /**
   * Eagerly connect to a list of endpoints.
   */
  def submit(task: => Future[Unit]): Unit = {
    val start = Time.now
    semaphore.acquireAndRun {
      val dur = (Time.now - start).inMilliseconds
      latencyStat.add(dur)

      task
    }
  }
}
