package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.stats.{Counter, Stat, StatsReceiver}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.{Duration, Memoize, Future, Return, Stopwatch, Throw}

/**
 * Statistic filter.
 *
 * Add counters:
 *    status.[code]
 *    status.[class]
 *    response_size (deprecated?)
 * And metrics:
 *    time.[code]
 *    time.[class]
 */
class StatsFilter[REQUEST <: Request](stats: StatsReceiver)
  extends SimpleFilter[REQUEST, Response] {

  private[this] val statusReceiver = stats.scope("status")
  private[this] val timeReceiver = stats.scope("time")
  private[this] val responseSizeStat = stats.stat("response_size")

  private[this] val counterCache: String => Counter =
    Memoize(statusReceiver.counter(_))

  private[this] val statCache: String => Stat =
    Memoize(timeReceiver.stat(_))

  def apply(request: REQUEST, service: Service[REQUEST, Response]): Future[Response] = {
    val elapsed = Stopwatch.start()
    val future = service(request)
    future respond {
      case Return(response) =>
        count(elapsed(), response)
      case Throw(_) =>
        // Treat exceptions as empty 500 errors
        val response = Response(request.version, Status.InternalServerError)
        count(elapsed(), response)
    }
    future
  }

  protected def count(duration: Duration, response: Response) {
    val statusCode = response.statusCode.toString
    val statusClass = (response.statusCode / 100).toString + "XX"

    counterCache(statusCode).incr()
    counterCache(statusClass).incr()

    statCache(statusCode).add(duration.inMilliseconds)
    statCache(statusClass).add(duration.inMilliseconds)

    responseSizeStat.add(response.length)
  }
}
