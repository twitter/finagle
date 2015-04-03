package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Timer, TimerTask}
import java.util.logging.Logger
import scala.collection.mutable

class JavaLoggerStatsReceiver(logger: Logger, timer: Timer)
  extends StatsReceiverWithCumulativeGauges
{
  val repr = logger
  private val timerTasks = new mutable.HashMap[Seq[String], TimerTask]

  // Timer here will never be released. This is ok since this class
  // is used for debugging only.
  def this(logger: Logger) = this(logger, DefaultTimer.twitter)

  def stat(name: String*): Stat = new Stat {
    def add(value: Float) {
      logger.info("%s add %f".format(formatName(name), value))
    }
  }

  def counter(name: String*): Counter = new Counter {
    def incr(delta: Int) {
      logger.info("%s incr %d".format(formatName(name), delta))
    }
  }

  protected[this] def registerGauge(name: Seq[String], f: => Float): Unit = synchronized {
    deregisterGauge(name)

    timerTasks(name) = timer.schedule(10.seconds) {
      logger.info("%s %2f".format(formatName(name), f))
    }
  }

  protected[this] def deregisterGauge(name: Seq[String]): Unit = synchronized {
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
