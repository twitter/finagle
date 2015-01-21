package com.twitter.finagle.param

import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.{stats, tracing, util, Stack}

/**
 * A class eligible for configuring a label used to identify finagle
 * clients and servers.
 */
case class Label(label: String)
object Label {
  implicit val param = new Stack.Param[Label] {
    val default = Label("")
  }
}

/**
 * A class eligible for configuring a [[com.twitter.util.Timer]] used
 * throughout finagle clients and servers.
 */
case class Timer(timer: com.twitter.util.Timer)
object Timer {
  implicit val param = new Stack.Param[Timer] {
    val default = Timer(util.DefaultTimer.twitter)
  }
}

/**
 * A class eligible for configuring a [[java.util.logging.Logger]]
 * used throughout finagle clients and servers.
 */
case class Logger(log: java.util.logging.Logger)
object Logger {
  implicit val param = new Stack.Param[Logger] {
    val default = Logger(util.DefaultLogger)
  }
}

/**
 * A class eligible for configuring a
 * [[com.twitter.finagle.stats.StatsReceiver]] throughout finagle
 * clients and servers.
 */
case class Stats(statsReceiver: stats.StatsReceiver)
object Stats {
  implicit val param = new Stack.Param[Stats] {
    // Note, this is lazy to avoid potential failures during
    // static initialization.
    lazy val default = Stats(stats.DefaultStatsReceiver)
  }
}

/**
 * A class eligible for configuring a [[com.twitter.util.Monitor]]
 * throughout finagle servers and clients.
 */
case class Monitor(monitor: com.twitter.util.Monitor)
object Monitor {
  implicit val param = new Stack.Param[Monitor] {
    // Note, this is lazy to avoid potential failures during
    // static initialization.
    lazy val default = Monitor(util.DefaultMonitor)
  }
}

/**
 * A class eligible for configuring a
 * [[com.twitter.finagle.util.ReporterFactory]] throughout finagle servers and
 * clients.
 */
case class Reporter(reporter: util.ReporterFactory)
object Reporter {
  implicit val param = new Stack.Param[Reporter] {
    // Note, this is lazy to avoid potential failures during
    // static initialization.
    lazy val default = Reporter(util.LoadedReporterFactory)
  }
}

/**
 * A class eligible for configuring a
 * [[com.twitter.finagle.tracing.Tracer]] throughout finagle servers
 * and clients.
 */
case class Tracer(tracer: tracing.Tracer)
object Tracer {
  implicit val param = new Stack.Param[Tracer] {
    // Note, this is lazy to avoid potential failures during
    // static initialization.
    lazy val default = Tracer(tracing.DefaultTracer)
  }
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
