package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.{Duration, Future, Return, Stopwatch, Throw}


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

    // TODO: Memoize on status code/class.
    statusReceiver.counter(statusCode).incr()
    statusReceiver.counter(statusClass).incr()

    timeReceiver.stat(statusCode).add(duration.inMilliseconds)
    timeReceiver.stat(statusClass).add(duration.inMilliseconds)

    responseSizeStat.add(response.length)
  }
}
