package com.twitter.finagle.stats

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.app.GlobalFlag
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.http.MediaType
import com.twitter.finagle.http.ParamMap
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.tunable.Tunable
import java.util.concurrent.atomic.AtomicBoolean
import java.io.IOException
import scala.collection.immutable
import scala.io.Codec
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.matching.Regex

object useCounterDeltas
    extends GlobalFlag[Boolean](
      false,
      "Return deltas for counters instead of absolute values. " +
        "Provides compatibility with the behavior from 'Ostrich'"
    )

object JsonExporter {
  // we cache this to make it easier to test JsonExporter, so that
  // we can check reference equality against this
  private[stats] val mapIdentity: collection.Map[String, Number] => collection.Map[String, Number] =
    identity

  private[stats] def startOfNextMinute: Time =
    Time.fromSeconds(Time.now.inMinutes * 60) + 1.minute

  private val log: Logger = Logger.get()

  /**
   * Merges individual regular expressions (represented as a sequence of strings) into
   * a [[Regex]] instance that is matched as long as one of these expressions is matched.
   */
  private def mergedRegex(regex: Seq[String]): Option[Regex] =
    if (regex.isEmpty) None
    else Some(regex.mkString("(", ")|(", ")").r)

  /**
   * Splits the given string by `,` and merges individual pieces into a [[Regex]].
   *
   * @see [[mergedRegex]]
   */
  private[stats] def commaSeparatedRegex(regex: String): Option[Regex] = {
    regex.split(",") match {
      case Array("") => None
      case regexes => mergedRegex(regexes)
    }
  }

  private def denylistDebugSample[A <: MetricsView.Snapshot](
    sample: Iterable[A],
    verbose: Option[String => Boolean]
  ): Iterable[A] =
    verbose match {
      case Some(pattern) =>
        sample.filter { value =>
          value.builder.verbosity != Verbosity.Debug || pattern(value.hierarchicalName)
        }

      case None =>
        sample.filter(_.builder.verbosity != Verbosity.Debug)
    }
}

/**
 * A Finagle HTTP service that exports [[Metrics]] (available via [[MetricsView]])
 * in a JSON format.
 *
 * This service respects metrics [[Verbosity verbosity levels]]: it doesn't export "debug"
 * (i.e., [[Verbosity.Debug]]) metrics unless they are allowlisted via the comma-separated
 * `verbose`.
 */
class JsonExporter(metrics: MetricsView, verbose: Tunable[String], timer: Timer)
    extends Service[Request, Response] { self =>

  import JsonExporter._

  def this(registry: MetricsView, timer: Timer) = this(
    registry,
    Verbose,
    timer
  )

  def this(registry: MetricsView) = this(registry, DefaultTimer)

  private[this] val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private[this] val writer = mapper.writer
  private[this] val prettyWriter = mapper.writer(new DefaultPrettyPrinter)

  private[this] def sampleVerbose(): Option[String] =
    verbose().orElse(com.twitter.finagle.stats.verbose.get)

  // scoped to stats for testing
  private[stats] lazy val filterSample: collection.Map[String, Number] => collection.Map[
    String,
    Number
  ] = {
    val regexesFromFile = statsFilterFile().flatMap { file =>
      try {
        Source.fromFile(file)(Codec.UTF8).getLines()
      } catch {
        case e: IOException =>
          log.error(e, "Unable to read statsFilterFile: %s", file)
          throw e
      }
    }
    val regexesFromFlag = statsFilter.get.toSeq.flatMap(_.split(","))
    mergedRegex(regexesFromFlag ++ regexesFromFile) match {
      case Some(regex) => new CachedRegex(regex)
      case None => mapIdentity
    }
  }

  private[this] val registryLoaded = new AtomicBoolean(false)

  // thread-safety provided by synchronization on `this`
  private[this] var deltas: Option[CounterDeltas] = None

  def apply(request: Request): Future[Response] = {
    if (registryLoaded.compareAndSet(false, true)) {
      GlobalRegistry.get
        .put(Seq("stats", "commons_metrics", "counters_latched"), useCounterDeltas().toString)
    }

    val response = Response()
    response.contentType = MediaType.Json

    val pretty = readBooleanParam(request.params, name = "pretty", default = false)
    val filtered = readBooleanParam(request.params, name = "filtered", default = false)
    val counterDeltasOn = {
      val vals = request.params.getAll("period")
      if (vals.isEmpty) {
        false
      } else {
        if (vals.exists(_ == "60")) true
        else {
          log.warning(
            s"${getClass.getName} request (from ${request.userAgent.getOrElse("")} ${request.remoteSocketAddress}) ignored due to unsupported period: '${vals
              .mkString(",")}'"
          )
          false
        }
      }
    }

    val asJson = json(pretty, filtered, counterDeltasOn)
    response.content = Buf.Utf8(asJson)
    Future.value(response)
  }

  // package protected for testing
  private[stats] def readBooleanParam(params: ParamMap, name: String, default: Boolean): Boolean = {
    val vals = params.getAll(name)
    if (vals.nonEmpty)
      vals.exists { v => v == "1" || v == "true" }
    else
      default
  }

  private[this] def getOrRegisterLatchedStats(): CounterDeltas = synchronized {
    deltas match {
      case Some(ds) => ds
      case None =>
        // Latching should happen every minute, at the top of the minute.
        deltas = Some(new CounterDeltas())
        timer.schedule(startOfNextMinute, 1.minute) {
          val ds = self.synchronized { deltas.get }
          ds.update(metrics.counters)
        }
        deltas.get
    }
  }

  def json(pretty: Boolean, filtered: Boolean, counterDeltasOn: Boolean = false): String = {
    val gauges: Iterable[GaugeSnapshot] =
      try metrics.gauges
      catch {
        case NonFatal(e) =>
          // because gauges run arbitrary user code, we want to protect ourselves here.
          // while the underlying registry should protect against individual misbehaving
          // gauges, an extra level of belt-and-suspenders seemed worthwhile.
          log.error(e, "exception while collecting gauges")
          Nil
      }
    val histos = metrics.histograms
    val counters = if (counterDeltasOn && useCounterDeltas()) {
      getOrRegisterLatchedStats().deltas
    } else {
      metrics.counters
    }

    // Converting a *-wildcard expression into a regular expression so we can match on it.
    val verbosePatten = sampleVerbose().map(Glob.apply)

    // We have to denylist debug metrics before we apply formatting, which may change
    // the names.
    val values = SampledValues(
      denylistDebugSample(gauges, verbosePatten),
      denylistDebugSample(counters, verbosePatten),
      denylistDebugSample(histos, verbosePatten)
    )

    val formatted = StatsFormatter.default(values)

    val sampleFiltered = if (filtered) filterSample(formatted) else formatted

    if (pretty) {
      // Create a TreeMap for sorting the keys
      val samples = immutable.TreeMap.empty[String, Number] ++ sampleFiltered
      prettyWriter.writeValueAsString(samples)
    } else {
      writer.writeValueAsString(sampleFiltered)
    }
  }
}
