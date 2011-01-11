package com.twitter.finagle.stats

import java.util.logging.Logger
import org.jboss.netty.util.HashedWheelTimer
import com.twitter.conversions.time._
import com.twitter.finagle.util.Conversions._

case class JavaLoggerStatsReceiver(logger: Logger) extends StatsReceiver {
  val timer = new HashedWheelTimer()

  def gauge(description: (String, String)*): Gauge = new super.Gauge {
    def measure(value: Float) {
      logger.info("%s measure %f".format(formatDescription(description), value))
    }
  }

  def counter(description: (String, String)*): Counter = new super.Counter {
    def incr(delta: Int) {
      logger.info("%s incr %d".format(formatDescription(description), delta))
    }
  }

  def mkGauge(name: Seq[(String, String)], f: => Float) {
    timer(10.seconds) {
      logger.info("%s %2f".format(name, f))
    }
  }

  private[this] def formatDescription(description: Seq[(String, String)]) = {
    description.map { case (key, value) =>
      "%s_%s".format(key, value)
    }.mkString("__")
  }
}

object JavaLoggerStatsReceiver {
  def apply(): JavaLoggerStatsReceiver = JavaLoggerStatsReceiver(Logger.getLogger(getClass.getName))
}
