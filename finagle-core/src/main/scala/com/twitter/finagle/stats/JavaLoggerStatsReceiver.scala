package com.twitter.finagle.stats

import java.util.logging.Logger
import org.jboss.netty.util.HashedWheelTimer
import com.twitter.conversions.time._
import com.twitter.finagle.util.Conversions._

case class JavaLoggerStatsReceiver(logger: Logger) extends StatsReceiver {
  val timer = new HashedWheelTimer()

  def observer(prefix: String, label: String) = {
    val suffix = "_%s".format(label)

    (path: Seq[String], value: Int, count: Int) => {
      val pathString = path mkString "__"
      logger.info(List(prefix, pathString, suffix, count) mkString " ")
    }
  }

  def makeGauge(name: String, f: => Float) {
    timer(10.seconds) {
      logger.info("%s %2f".format(name, f))
      makeGauge(name, f)
    }
  }
}

object JavaLoggerStatsReceiver {
  def apply(): JavaLoggerStatsReceiver = JavaLoggerStatsReceiver(Logger.getLogger(getClass.getName))
}
