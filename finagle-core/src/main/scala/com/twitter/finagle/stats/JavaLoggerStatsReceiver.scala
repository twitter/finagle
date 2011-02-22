package com.twitter.finagle.stats

import java.util.logging.Logger
import com.twitter.conversions.time._
import com.twitter.util
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.Timer

class JavaLoggerStatsReceiver(logger: Logger, timer: util.Timer) extends StatsReceiver {
  def this(logger: Logger) = this(logger, Timer.default)

  def stat(name: String*) = new Stat {
    def add(value: Float) {
      logger.info("%s add %f (%d)".format(formatName(name), value))
    }
  }

  def counter(name: String*) = new Counter {
    def incr(delta: Int) {
      logger.info("%s incr %d".format(formatName(name), delta))
    }
  }

  def provideGauge(name: String*)(f: => Float) {
    timer.schedule(10.seconds) {
      logger.info("%s %2f".format(formatName(name), f))
    }
  }

  private[this] def formatName(description: Seq[String]) = {
    description mkString "/"
  }
}

object JavaLoggerStatsReceiver {
  def apply(): JavaLoggerStatsReceiver =
    new JavaLoggerStatsReceiver(Logger.getLogger("Finagle"))
}
