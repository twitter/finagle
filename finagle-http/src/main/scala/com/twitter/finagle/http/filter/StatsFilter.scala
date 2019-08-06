package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.stats.{Counter, Stat, StatsReceiver}
import com.twitter.util._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReferenceArray

object StatsFilter {
  val role: Stack.Role = Stack.Role("HttpStatsFilter")
  val description: String = "HTTP Stats"

  def module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module1[param.Stats, ServiceFactory[Request, Response]] {
      val role: Stack.Role = StatsFilter.role
      val description: String = StatsFilter.description

      def make(
        statsParam: param.Stats,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] = {
        if (statsParam.statsReceiver.isNull)
          next
        else
          new StatsFilter[Request](statsParam.statsReceiver.scope("http")).andThen(next)
      }
    }

  private def statusCodeRange(code: Int): String =
    if (code < 100) "UNKNOWN"
    else if (code < 200) "1XX"
    else if (code < 300) "2XX"
    else if (code < 400) "3XX"
    else if (code < 500) "4XX"
    else if (code < 600) "5XX"
    else "UNKNOWN"

  private def getStatusRangeIndex(code: Int): Int =
    if (code < 100) 0
    else if (code < 200) 1
    else if (code < 300) 2
    else if (code < 400) 3
    else if (code < 500) 4
    else if (code < 600) 5
    else 0

  private val nowTimeInMillis: () => Long = Stopwatch.systemMillis

  private case class StatusStats(count: Counter, latency: Stat)

}

/**
 * Statistic filter.
 *
 * Add counters:
 *    status.[code]
 *    status.[class]
 * And metrics:
 *    time.[code]
 *    time.[class]
 */
class StatsFilter[REQUEST <: Request] private[filter] (stats: StatsReceiver, now: () => Long)
    extends SimpleFilter[REQUEST, Response] {

  import StatsFilter._

  def this(stats: StatsReceiver) = this(stats, StatsFilter.nowTimeInMillis)

  private[this] val statusReceiver = stats.scope("status")
  private[this] val timeReceiver = stats.scope("time")

  // Optimized for the known valid status code ranges (1xx, 2xx, 3xx, 4xx, 5xx)
  // where the 0th index represents an unknown/invalid HTTP status range.
  // We initialize these lazily to prevent unused metrics from being exported.
  private[this] val statusRange: AtomicReferenceArray[StatusStats] =
    new AtomicReferenceArray[StatusStats](6)

  private[this] def getStatusRange(code: Int): StatusStats = {
    val index = getStatusRangeIndex(code)
    val statsPair = statusRange.get(index)
    if (statsPair == null) {
      val codeRange = statusCodeRange(code)
      val initPair = StatusStats(statusReceiver.counter(codeRange), timeReceiver.stat(codeRange))
      statusRange.compareAndSet(index, null, initPair)
      initPair
    } else {
      statsPair
    }
  }

  private[this] val statusCache = new ConcurrentHashMap[Int, StatusStats]()
  private[this] val statusCacheFn = new java.util.function.Function[Int, StatusStats] {
    def apply(t: Int): StatusStats = {
      val statusCode = t.toString
      StatusStats(statusReceiver.counter(statusCode), timeReceiver.stat(statusCode))
    }
  }

  def apply(request: REQUEST, service: Service[REQUEST, Response]): Future[Response] = {
    val start = now()

    val future = service(request)
    future respond {
      case Return(response) =>
        count(now() - start, response)
      case Throw(_) =>
        // Treat exceptions as empty 500 errors
        val response = Response(request.version, Status.InternalServerError)
        count(now() - start, response)
    }
    future
  }

  private[this] def count(durationMs: Long, response: Response): Unit = {
    val statusCode = response.statusCode

    // we don't use a named pair to avoid extra allocation here
    // ex: `val (counter, stat) = getStatusRange(statusCode)`
    val rangeStats = getStatusRange(statusCode)
    rangeStats.count.incr()
    rangeStats.latency.add(durationMs)

    val codeStats = statusCache.computeIfAbsent(statusCode, statusCacheFn)
    codeStats.count.incr()
    codeStats.latency.add(durationMs)
  }
}
