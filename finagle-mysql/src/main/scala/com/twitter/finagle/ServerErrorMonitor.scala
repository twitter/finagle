package com.twitter.finagle

import com.twitter.finagle.mysql.ServerError
import com.twitter.util.Monitor

/**
 * ServerErrorMonitor is a [[com.twitter.util.Monitor]] that specifically handles mysql ServerErrors
 * in a less verbose way then [[com.twitter.finagle.util.DefaultMonitor]].
 *
 * By default it will log an info statement with details about the ServerError and encourage end
 * users to explicitly `handle` or `rescue` the ServerError in application code, and then suppress
 * the logging.
 *
 * Specific error codes can be suppressed, with the intention being that this happens when application
 * code is explicitly dealing with that ServerError variant.
 *
 * @example {{{
 * val client = Mysql.client
 *   .withMonitor(ServerErrorMonitor(Seq(<code>)))
 * }}}
 */
case class ServerErrorMonitor(suppressedCodes: Seq[Short]) extends Monitor {
  private val log = com.twitter.logging.Logger(classOf[ServerErrorMonitor])

  override def handle(exc: scala.Throwable): Boolean = {
    exc match {
      case ServerError(code, _, _) if suppressedCodes.contains(code) =>
        true
      case ServerError(code, sqlState, message) =>
        log.info(
          s"caught unsuppressed ServerError($code, $sqlState, $message); consider using `handle` or `rescue` in application code and then suppressing this statement via `.withMonitor(ServerErrorMonitor(Seq(<code>)))`")
        true
      case _ => false
    }
  }
}
