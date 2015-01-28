package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.{param, ServiceFactory, Stack, Stackable, StackBuilder}
import com.twitter.finagle.pool.{WatermarkPool, CachingPool, BufferingPool}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Timer, Duration}

object DefaultPool {

  implicit object Role extends Stack.Role("Pool") {
    val bufferingPool = Stack.Role("BufferingPool")
    val cachingPool = Stack.Role("CachingPool")
    val watermarkPool = Stack.Role("WatermarkPool")
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * default pool module.
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
  case class Param(low: Int, high: Int, bufferSize: Int, idleTime: Duration, maxWaiters: Int) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }
  object Param {
    implicit val param = Stack.Param(Param(0, Int.MaxValue, 0, Duration.Top, Int.MaxValue))
  }

  /**
   * A [[com.twitter.finagle.Stackable]] client connection pool.
   *
   * @see [[com.twitter.finagle.pool.BufferingPool]].
   * @see [[com.twitter.finagle.pool.WatermarkPool]].
   * @see [[com.twitter.finagle.pool.CachingPool]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      import com.twitter.finagle.pool.{CachingPool, WatermarkPool, BufferingPool}
      val role = DefaultPool.Role
      val description = "Control client connection pool"
      val parameters = Seq(
        implicitly[Stack.Param[Param]],
        implicitly[Stack.Param[param.Stats]],
        implicitly[Stack.Param[param.Timer]])
      def make(prms: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val Param(low, high, bufferSize, idleTime, maxWaiters) = prms[Param]
        val param.Stats(statsReceiver) = prms[param.Stats]
        val param.Timer(timer) = prms[param.Timer]

        val stack = new StackBuilder[ServiceFactory[Req, Rep]](next)

        if (idleTime > 0.seconds && high > low) {
          stack.push(Role.cachingPool, (sf: ServiceFactory[Req, Rep]) =>
            new CachingPool(sf, high-low, idleTime, timer, statsReceiver))
        }

        stack.push(Role.watermarkPool, (sf: ServiceFactory[Req, Rep]) =>
          new WatermarkPool(sf, low, high, statsReceiver, maxWaiters))

        if (bufferSize > 0) {
          stack.push(Role.bufferingPool, (sf: ServiceFactory[Req, Rep]) =>
            new BufferingPool(sf, bufferSize))
        }

        stack.result
      }
    }
}

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

    // NB: WatermarkPool conceals the first "low" closes from CachingPool, so that
    // CachingPool only caches the last "high - low", and WatermarkPool caches the first
    // "low".
    val pool = new WatermarkPool(factory, low, high, statsReceiver, maxWaiters)
    if (bufferSize <= 0) pool else new BufferingPool(pool, bufferSize)
  }
}
