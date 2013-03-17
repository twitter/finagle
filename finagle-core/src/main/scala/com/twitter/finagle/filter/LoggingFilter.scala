package com.twitter.finagle.filter

import com.twitter.util.{Duration, Return, Throw, Stopwatch, Future}
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.logging.Logger

trait LogFormatter[Req, Rep] {
  def format(request: Req, response: Rep, responseTime: Duration): String

  def formatException(request: Req, throwable: Throwable, responseTime: Duration): String
}

/**
 *  Logging filter.
 *
 * Logs all requests according to formatter.
 */
trait LoggingFilter[Req, Rep] extends SimpleFilter[Req, Rep] {
  val log: Logger
  val formatter: LogFormatter[Req, Rep]

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val elapsed = Stopwatch.start()
    val future = service(request)
    future respond {
      case Return(response) =>
        log(elapsed(), request, response)
      case Throw(throwable) =>
        logException(elapsed(), request, throwable)
    }
    future
  }

  protected def log(responseTime: Duration, request: Req, response: Rep) {
    val line = formatter.format(request, response, responseTime)
    log.info(line)
  }

  protected def logException(responseTime: Duration, request: Req, throwable: Throwable) {
    val line = formatter.formatException(request, throwable, responseTime)
    log.info(throwable, line)
  }

}
