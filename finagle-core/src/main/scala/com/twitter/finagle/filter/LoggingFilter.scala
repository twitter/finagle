package com.twitter.finagle.filter

import com.twitter.util.{Duration, Return, Throw, Stopwatch, Future}
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.logging.Logger

trait LogFormatter[-Req, Rep] {
  def format(request: Req, reply: Rep, replyTime: Duration): String

  def formatException(request: Req, throwable: Throwable, replyTime: Duration): String
}

/**
 * A [[com.twitter.finagle.Filter]] that logs all requests according to
 * formatter.
 */
trait LoggingFilter[Req, Rep] extends SimpleFilter[Req, Rep] {
  val log: Logger
  val formatter: LogFormatter[Req, Rep]

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val elapsed = Stopwatch.start()
    val future = service(request)
    future respond {
      case Return(reply) =>
        log(elapsed(), request, reply)
      case Throw(throwable) =>
        logException(elapsed(), request, throwable)
    }
    future
  }

  protected def log(replyTime: Duration, request: Req, reply: Rep) {
    val line = formatter.format(request, reply, replyTime)
    log.info(line)
  }

  protected def logException(replyTime: Duration, request: Req, throwable: Throwable) {
    val line = formatter.formatException(request, throwable, replyTime)
    log.info(throwable, line)
  }

}
