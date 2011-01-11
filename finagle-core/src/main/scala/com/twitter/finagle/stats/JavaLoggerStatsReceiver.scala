package com.twitter.finagle.stats

import java.util.logging.Logger
import com.twitter.conversions.time._
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.Timer

class JavaLoggerStatsReceiver(logger: Logger, timer: Timer) extends StatsReceiver {
  def this(logger: Logger) = this(logger, Timer.default)

  def gauge(description: (String, String)*) = new Gauge {
    def measure(value: Float) {
      logger.info("%s measure %f".format(formatDescription(description), value))
    }
  }

  def counter(description: (String, String)*) = new Counter {
    def incr(delta: Int) {
      logger.info("%s incr %d".format(formatDescription(description), delta))
    }
  }

  def mkGauge(name: Seq[(String, String)], f: => Float) {
    timer.schedule(10.seconds) {
      logger.info("%s %2f".format(name, f))
      makeGauge(name, f)
    }
  }

  private[this] def formatDescription(description: Seq[(String, String)]) = {
    description.map { case (key, value) =>
      "%s_%s".format(key, value)
    }.mkString("__")
  }
}

object JavaLoggerStatsReceiver {
  def apply(): JavaLoggerStatsReceiver =
    new JavaLoggerStatsReceiver(Logger.getLogger("Finagle"))
}
