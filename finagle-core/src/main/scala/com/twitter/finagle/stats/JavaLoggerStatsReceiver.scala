package com.twitter.finagle.stats

import com.twitter.conversions.DurationOps._
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

  def stat(schema: HistogramSchema): Stat = new Stat {
    def add(value: Float): Unit = {
      val level =
        if (schema.metricBuilder.verbosity == Verbosity.Debug) Level.FINEST else Level.INFO
      val formattedName = formatName(schema.metricBuilder.name)
      logger.log(level, s"$formattedName add $value")
    }
    def metadata: Metadata = schema.metricBuilder
  }

  def counter(schema: CounterSchema): Counter = new Counter {
    def incr(delta: Long): Unit = {
      val level =
        if (schema.metricBuilder.verbosity == Verbosity.Debug) Level.FINEST else Level.INFO
      val formattedName = formatName(schema.metricBuilder.name)
      logger.log(level, s"$formattedName incr $delta")
    }
    def metadata: Metadata = schema.metricBuilder
  }

  override def addGauge(schema: GaugeSchema)(f: => Float): Gauge = {
    registerGauge(schema, f)

    // placeholder gauge that just supplies metadata
    new Gauge {
      def remove(): Unit = ()
      def metadata: Metadata = schema.metricBuilder
    }
  }

  protected[this] def registerGauge(schema: GaugeSchema, f: => Float): Unit =
    synchronized {
      deregisterGauge(schema.metricBuilder.name)

      val level =
        if (schema.metricBuilder.verbosity == Verbosity.Debug) Level.FINEST else Level.INFO

      timerTasks(schema.metricBuilder.name) = timer.schedule(10.seconds) {
        logger.log(level, "%s %2f".format(formatName(schema.metricBuilder.name), f))
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
