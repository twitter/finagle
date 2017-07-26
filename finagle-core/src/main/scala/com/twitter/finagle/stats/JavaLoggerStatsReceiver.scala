package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Timer, TimerTask}
import java.util.logging.{Level, Logger}
import scala.collection.mutable

class JavaLoggerStatsReceiver(logger: Logger, timer: Timer)
    extends StatsReceiverWithCumulativeGauges {
  val repr = logger
  private val timerTasks = new mutable.HashMap[Seq[String], TimerTask]

  // Timer here will never be released. This is ok since this class
  // is used for debugging only.
  def this(logger: Logger) = this(logger, DefaultTimer)

  def stat(verbosity: Verbosity, name: String*): Stat = new Stat {
    def add(value: Float) {
      val level = if (verbosity == Verbosity.Debug) Level.FINEST else Level.INFO
      logger.log(level, "%s add %f".format(formatName(name), value))
    }
  }

  def counter(verbosity: Verbosity, name: String*): Counter = new Counter {
    def incr(delta: Long) {
      val level = if (verbosity == Verbosity.Debug) Level.FINEST else Level.INFO
      logger.log(level, "%s incr %d".format(formatName(name), delta))
    }
  }

  protected[this] def registerGauge(verbosity: Verbosity, name: Seq[String], f: => Float): Unit =
    synchronized {
      deregisterGauge(name)

      val level = if (verbosity == Verbosity.Debug) Level.FINEST else Level.INFO

      timerTasks(name) = timer.schedule(10.seconds) {
        logger.log(level, "%s %2f".format(formatName(name), f))
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
