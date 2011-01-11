package com.twitter.finagle.stats

import java.util.logging.Logger
import org.jboss.netty.util.HashedWheelTimer
import com.twitter.conversions.time._
import com.twitter.finagle.util.Conversions._

case class JavaLoggerStatsReceiver(logger: Logger) extends StatsReceiver {
  val timer = new HashedWheelTimer()

  private[this] class Counter(description: Seq[(String, String)]) extends OCounter {
    def incr(delta: Int) {
      logger.info("%s incr %d".format(formatDescription(description), delta))
    }

    val sum = 0
  }

  private[this] class Gauge(description: Seq[(String, String)]) extends OGauge {
    def measure(value: Float) {
      logger.info("%s measure %f".format(formatDescription(description), value))
    }

    val summary = Summary(0.0f, 0)
  }

  def gauge(description: (String, String)*): OGauge = new Gauge(description)
  def counter(description: (String, String)*): OCounter = new Counter(description)

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
