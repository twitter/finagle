package com.twitter.finagle.builder

import com.twitter.finagle.{Failure, SourcedException}
import com.twitter.logging.HasLogLevel
import com.twitter.util.Monitor
import java.io.IOException
import java.util.logging.{Level, Logger}

/**
 * A monitor that unrolls the exception causes to report source information if any
 * @param which either client or server
 */
class SourceTrackingMonitor(logger: Logger, which: String) extends Monitor {
  def handle(exc: Throwable): Boolean = {
    // much like ChannelStatsHandler.exceptionCaught,
    // we don't want these noisy IOExceptions leaking into the logs.
    val level = exc match {
      case _: IOException => Level.FINE
      case f: HasLogLevel => f.logLevel
      case _ => Level.SEVERE
    }
    logger.log(
      level,
      "A " + which + " service " +
        unrollCauses(exc).mkString(" on behalf of ") + " threw an exception",
      exc)
    false
  }

  private[this] def unrollCauses(exc: Throwable, res: Seq[String] = Nil): Seq[String] = exc match {
    case null => res.reverse
    case se: SourcedException => unrollCauses(se.getCause, se.serviceName +: res)
    case fail: Failure => fail.getSource(Failure.Source.Service) match {
      case Some(name) => unrollCauses(fail.getCause, name.toString +: res)
      case _ => unrollCauses(fail.getCause, res)
    }
    case _ => unrollCauses(exc.getCause, res)
  }
}
