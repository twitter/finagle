package com.twitter.finagle.param

import com.twitter.finagle.service.CoreMetricsRegistry
import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.stats.Disabled
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.Stack
import com.twitter.finagle.stats
import com.twitter.finagle.tracing
import com.twitter.finagle.util
import com.twitter.util.JavaTimer
import com.twitter.util.NullMonitor
import scala.annotation.varargs
import scala.language.reflectiveCalls

/**
 * A class eligible for configuring a label used to identify finagle
 * clients and servers.
 */
case class Label(label: String) {
  def mk(): (Label, Stack.Param[Label]) =
    (this, Label.param)
}
object Label {
  private[finagle] val Default: String = ""

  implicit val param: Stack.Param[Label] = Stack.Param(Label(Default))
}

/**
 * Tags are a more powerful Label.
 *
 * Tags associate Finagle clients and servers with a set of keywords. Labels
 * are simply Tags with a single keyword.
 *
 * Tags provide a general purpose configuration mechanism for functionality
 * that is not yet known. This is powerful, but also easily misused. As such,
 * be conservative in using them.
 *
 * Frameworks that create services for each endpoint should tag them with
 * endpoint metadata, e.g.,
 *
 * {{{
 * val showPost = Http.server.withLabels("GET", "/posts/show")
 * val createPost = Http.server.withLabels("POST", "PUT", "/posts/create")
 * }}}
 *
 * Note: Tags can't be used in place of Label (at least not quite yet). Label
 * will appear in metrics, but Tags do not.
 */
private[twitter] sealed abstract class Tags {
  import Tags._

  def mk(): (Tags, Stack.Param[Tags]) =
    (this, Tags.param)

  /**
   * True, if Tags contains any of keyword.
   */
  @varargs
  def matchAny(keyword: String*): Boolean = this match {
    case tags @ KeySet(_) => tags.keys.intersect(keyword.toSet).nonEmpty
  }

  /**
   * True, if Tags contains all these keywords.
   */
  @varargs
  def matchAll(keywords: String*): Boolean = this match {
    case tags @ KeySet(_) => keywords.toSet.subsetOf(tags.keys)
  }
}
private[twitter] object Tags {
  implicit val param = Stack.Param(Tags())

  private case class KeySet(keys: Set[String]) extends Tags

  @varargs
  def apply(keys: String*): Tags = KeySet(keys.toSet)
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
  implicit val param: Stack.Param[ProtocolLibrary] = Stack.Param(ProtocolLibrary("not-specified"))
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
  implicit val param: Stack.Param[Timer] = Stack.Param(Timer(DefaultTimer))
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
   * The underlying default Timer used for configuration
   * Default `stop` behaviour is ignored in order to not be accidentally stopped
   */
  private[this] val underlying = new JavaTimer(true, Some("HighResTimer")) {
    override def stop(): Unit = ()
    def stopTimer(): Unit = super.stop()
  }

  /**
   * The default Timer used for configuration.
   *
   * It is a shared resource and as such, `stop` is ignored.
   */
  val Default: com.twitter.util.Timer = underlying

  /**
   * Stop default HighResTimer
   */
  def stop(): Unit = {
    com.twitter.logging.Logger
      .get().warning("Stopping the default Finagle HighResTimer. When timer is stopped, " +
        "the behaviors of Finagle client and server are undefined.")
    underlying.stopTimer()
  }

  implicit val param: Stack.Param[HighResTimer] =
    Stack.Param(HighResTimer(Default))
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
  implicit val param: Stack.Param[Stats] = Stack.Param(Stats(stats.DefaultStatsReceiver))
}

/**
 * A class eligible for configuring a
 * [[com.twitter.finagle.stats.StandardStats]] throughout finagle
 * clients and servers.
 * @see [[stats.StandardStatsReceiver]]
 */
private[finagle] case class StandardStats(standardStats: stats.StandardStats) {
  def mk(): (StandardStats, Stack.Param[StandardStats]) = (this, StandardStats.param)
}
private[finagle] object StandardStats {
  implicit val param: Stack.Param[StandardStats] =
    Stack.Param(StandardStats(Disabled))
}

/**
 * A class eligible for configuring a [[com.twitter.finagle.service.CoreMetricsRegistry]]
 * throughout finagle servers. The CoreMetricsRegistry allows for constructing a set of
 * essential metrics expressions, [[CoreMetricsRegistry]] is a stateful class and should
 * be configured per-stack.
 */
case class MetricBuilders(registry: Option[CoreMetricsRegistry]) {
  def mk(): (MetricBuilders, Stack.Param[MetricBuilders]) =
    (this, MetricBuilders.param)
}

object MetricBuilders {
  implicit val param: Stack.Param[MetricBuilders] =
    Stack.Param(MetricBuilders(None))
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
  implicit val param: Stack.Param[Monitor] = Stack.Param(Monitor(NullMonitor))
}

/**
 * A class eligible for configuring a [[com.twitter.finagle.service.ResponseClassifier]]
 * which is used to determine the result of a request/response.
 *
 * This allows developers to give Finagle the additional application specific
 * knowledge necessary in order to properly classify them. Without this,
 * Finagle can only safely make judgements about the transport level failures.
 *
 * As an example take an HTTP client that receives a response with a 500 status
 * code back from a server. To Finagle this is a successful request/response
 * based solely on the transport level. The application developer may want to
 * treat all 500 status codes as failures and can do so via a
 * [[com.twitter.finagle.service.ResponseClassifier]].
 *
 * It is a [[PartialFunction]] and as such multiple classifiers can be composed
 * together via [[PartialFunction.orElse]].
 *
 * @see `com.twitter.finagle.http.service.HttpResponseClassifier` for some
 * HTTP classification tools.
 *
 * @note If unspecified, the default classifier is
 * [[com.twitter.finagle.service.ResponseClassifier.Default]]
 * which is a total function fully covering the input domain.
 */
case class ResponseClassifier(responseClassifier: com.twitter.finagle.service.ResponseClassifier) {
  def mk(): (ResponseClassifier, Stack.Param[ResponseClassifier]) =
    (this, ResponseClassifier.param)
}
object ResponseClassifier {
  implicit val param: Stack.Param[ResponseClassifier] =
    Stack.Param(ResponseClassifier(com.twitter.finagle.service.ResponseClassifier.Default))
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
  implicit val param: Stack.Param[Reporter] = Stack.Param(Reporter(util.LoadedReporterFactory))
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
  implicit val param: Stack.Param[Tracer] = Stack.Param(Tracer(tracing.DefaultTracer))
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
  implicit val param: Stack.Param[ExceptionStatsHandler] = new Stack.Param[ExceptionStatsHandler] {
    // Note, this is lazy to avoid potential failures during
    // static initialization.
    lazy val default = ExceptionStatsHandler(StatsFilter.DefaultExceptions)
  }
}
