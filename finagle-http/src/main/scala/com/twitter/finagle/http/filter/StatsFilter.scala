package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.{Duration, Future, Return, Stopwatch, Throw}


/** Statistic filter.
  * Add counters:
  *    status.<code>
  *    status.<class>
  *    response_size (deprecated?)
  * And metrics:
  *    time.<code>
  *    time.<class>
  */
class StatsFilter[REQUEST <: Request](stats: StatsReceiver)
  extends SimpleFilter[REQUEST, Response] {

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
    stats.scope("status").counter(statusCode).incr()
    stats.scope("status").counter(statusClass).incr()

    stats.scope("time").stat(statusCode).add(duration.inMilliseconds)
    stats.scope("time").stat(statusClass).add(duration.inMilliseconds)

    stats.stat("response_size").add(response.length)
  }
}
