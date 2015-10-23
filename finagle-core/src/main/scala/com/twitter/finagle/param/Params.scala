package com.twitter.finagle.param

import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.util.DefaultMonitor
import com.twitter.finagle.{stats, tracing, util, Stack}
import com.twitter.util.JavaTimer

/**
 * A class eligible for configuring a label used to identify finagle
 * clients and servers.
 */
case class Label(label: String) {
  def mk(): (Label, Stack.Param[Label]) =
    (this, Label.param)
}
object Label {
  implicit val param = Stack.Param(Label(""))
}

/**
 * A class eligible for configuring a client library name used to identify
 * which client library a client is using.
 */
case class ProtocolLibrary(name: String) {
  def mk(): (ProtocolLibrary, Stack.Param[ProtocolLibrary]) =
    (this, ProtocolLibrary.param)
}
object ProtocolLibrary {
  implicit val param = Stack.Param(ProtocolLibrary("not-specified"))
}

/**
 * A class eligible for configuring a [[com.twitter.util.Timer]] used
 * throughout finagle clients and servers.
 *
 * @param timer it is a requirement that it propagates [[com.twitter.util.Local Locals]]
 *  from scheduling time to execution time.
 *
 * @see [[HighResTimer]] for a configuration that needs a more
 *   fine-grained timer as this is typically implemented via a
 *   "hashed wheel timer" which is optimized for approximated
 *   I/O timeout scheduling.
 */
case class Timer(timer: com.twitter.util.Timer) {
  def mk(): (Timer, Stack.Param[Timer]) =
    (this, Timer.param)
}
object Timer {
  implicit val param = Stack.Param(Timer(util.DefaultTimer.twitter))
}

/**
 * A class eligible for configuring a high resolution [[com.twitter.util.Timer]]
 * such that tasks are run tighter to their schedule.
 *
 * @param timer it is a requirement that it propagates [[com.twitter.util.Local Locals]]
 *  from scheduling time to execution time.
 *
 * @see [[Timer]] for a configuration that is appropriate for
 *   tasks that do not need fine-grained scheduling.
 *
 * @note it is expected that the resolution should be sub-10 milliseconds.
 */
case class HighResTimer(timer: com.twitter.util.Timer) {
  def mk(): (HighResTimer, Stack.Param[HighResTimer]) =
    (this, HighResTimer.param)
}

object HighResTimer {
  /**
   * The default Timer used for configuration.
   *
   * It is a shared resource and as such, `stop` is ignored.
   */
  val Default: com.twitter.util.Timer =
    new JavaTimer(true, Some("HighResTimer")) {
      override def stop(): Unit = ()
    }

  implicit val param = Stack.Param(HighResTimer(Default))
}

/**
 * A class eligible for configuring a [[java.util.logging.Logger]]
 * used throughout finagle clients and servers.
 */
case class Logger(log: java.util.logging.Logger) {
  def mk(): (Logger, Stack.Param[Logger]) =
    (this, Logger.param)
}
object Logger {
  implicit val param = Stack.Param(Logger(util.DefaultLogger))
}

/**
 * A class eligible for configuring a
 * [[com.twitter.finagle.stats.StatsReceiver]] throughout finagle
 * clients and servers.
 */
case class Stats(statsReceiver: stats.StatsReceiver) {
  def mk(): (Stats, Stack.Param[Stats]) =
    (this, Stats.param)
}
object Stats {
  implicit val param = Stack.Param(Stats(stats.DefaultStatsReceiver))
}

/**
 * A class eligible for configuring a [[com.twitter.util.Monitor]]
 * throughout finagle servers and clients.
 */
case class Monitor(monitor: com.twitter.util.Monitor) {
  def mk(): (Monitor, Stack.Param[Monitor]) =
    (this, Monitor.param)
}
object Monitor {
  implicit val param = Stack.Param(Monitor(DefaultMonitor))
}

/**
 * A class eligible for configuring a
 * [[com.twitter.finagle.util.ReporterFactory]] throughout finagle servers and
 * clients.
 */
case class Reporter(reporter: util.ReporterFactory) {
  def mk(): (Reporter, Stack.Param[Reporter]) =
    (this, Reporter.param)
}
object Reporter {
  implicit val param = Stack.Param(Reporter(util.LoadedReporterFactory))
}

/**
 * A class eligible for configuring a
 * [[com.twitter.finagle.tracing.Tracer]] throughout finagle servers
 * and clients.
 */
case class Tracer(tracer: tracing.Tracer) {
  def mk(): (Tracer, Stack.Param[Tracer]) =
    (this, Tracer.param)
}
object Tracer {
  implicit val param = Stack.Param(Tracer(tracing.DefaultTracer))
}

/**
 * A class eligible for configuring a
 * [[com.twitter.finagle.stats.ExceptionStatsHandler]] throughout finagle servers
 * and clients.
 *
 * NB: Since the default for failures is to be scoped under "failures", if you
 * set the default to be in another scope, it may be difficult for engineers
 * unfamiliar with your stats to understand your service's key metrics.
 */
case class ExceptionStatsHandler(categorizer: stats.ExceptionStatsHandler)
object ExceptionStatsHandler {
  implicit val param = new Stack.Param[ExceptionStatsHandler] {
    // Note, this is lazy to avoid potential failures during
    // static initialization.
    lazy val default = ExceptionStatsHandler(StatsFilter.DefaultExceptions)
  }
}
