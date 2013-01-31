package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.pool.{WatermarkPool, CachingPool, BufferingPool}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Timer, Duration}

/**
 * Create a watermark pool backed by a caching pool. This is the
 * default pooling setup of Finagle.
 *
 * @param low The low watermark used in the Watermark pool. If there
 * is sufficient request concurrency, no fewer connections will be
 * maintained by the pool.
 *
 * @param high The high watermark. The pool will not maintain more
 * connections than this.
 *
 * @param bufferSize Specifies the size of the lock-free buffer in front of
 * the pool configuration. Skipped if 0.
 *
 * @param idleTime The amount of idle time for which a connection is
 * cached. This is applied to connections that number greater than
 * the low watermark but fewer than the high.
 *
 * @param maxWaiters The maximum number of connection requests that
 * are queued when the connection concurrency exceeds the high
 * watermark.
 */
case class DefaultPool[Req, Rep](
    low: Int = 0,
    high: Int = Int.MaxValue,
    bufferSize: Int = 0,
    idleTime: Duration = Duration.Top,
    maxWaiters: Int = Int.MaxValue,
    timer: Timer = DefaultTimer.twitter
) extends (StatsReceiver => Transformer[Req, Rep]) {
  def apply(statsReceiver: StatsReceiver) = inputFactory => {
    val factory =
      if (idleTime <= 0.seconds || high <= low) inputFactory else
        new CachingPool(inputFactory, high - low, idleTime, timer, statsReceiver)

    val pool = new WatermarkPool(factory, low, high, statsReceiver, maxWaiters)
    if (bufferSize <= 0) pool else new BufferingPool(pool, bufferSize)
  }
}
