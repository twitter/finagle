package com.twitter.finagle.builder

import com.twitter.finagle.SourcedException
import com.twitter.util.Monitor
import java.util.logging.{Level, Logger}

/**
 * A monitor that unrolls the exception causes to report source information if any
 */
class SourceTrackingMonitor(logger: Logger) extends Monitor {
  def handle(exc: Throwable) = {
    logger.log(
      Level.SEVERE,
      "A Service " + unrollCauses(exc).mkString(" on behalf of ") + " threw an exception", exc)
    false
  }

  private[this] def unrollCauses(exc: Throwable, res: Seq[String] = Seq()): Seq[String] = exc match {
    case null => res.reverse
    case se: SourcedException => unrollCauses(se.getCause, se.serviceName +: res)
    case _ => unrollCauses(exc.getCause, res)
  }
}
