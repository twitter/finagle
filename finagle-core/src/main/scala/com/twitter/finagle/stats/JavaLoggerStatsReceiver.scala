package com.twitter.finagle.stats

import collection.mutable.HashMap

import java.util.logging.Logger
import com.twitter.conversions.time._
import com.twitter.util
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.Timer

class JavaLoggerStatsReceiver(logger: Logger, timer: util.Timer)
  extends StatsReceiverWithCumulativeGauges
{
  var timerTasks = new HashMap[Seq[String], util.TimerTask]

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

  protected[this] def registerGauge(name: Seq[String], f: => Float) = synchronized {
    deregisterGauge(name)

    timerTasks(name) = timer.schedule(10.seconds) {
      logger.info("%s %2f".format(formatName(name), f))
    }
  }

  protected[this] def deregisterGauge(name: Seq[String]) {
    timerTasks.remove(name) foreach { _.cancel() }
  }

  private[this] def formatName(description: Seq[String]) = {
    description mkString "/"
  }
}

object JavaLoggerStatsReceiver {
  def apply(): JavaLoggerStatsReceiver =
    new JavaLoggerStatsReceiver(Logger.getLogger("Finagle"))
}
