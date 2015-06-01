package com.twitter.finagle.stats

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.annotations.VisibleForTesting
import com.twitter.app.GlobalFlag
import com.twitter.common.metrics.Metrics
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.httpx.{RequestParamMap, Response, Request, MediaType}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.util.registry.GlobalRegistry
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.immutable
import scala.util.matching.Regex

/**
 * Blacklist of regex, comma-separated. Comma is a reserved character and cannot be used.
 *
 * See http://www.scala-lang.org/api/current/#scala.util.matching.Regex
 */
object statsFilter extends GlobalFlag[String](
  "",
  "Comma-separated regexes that indicate which metrics to filter out")

object useCounterDeltas extends GlobalFlag[Boolean](
  false,
  "Return deltas for counters instead of absolute values. " +
    "Provides compatibility with the behavior from 'Ostrich'"
)

object JsonExporter {

  @VisibleForTesting
  private[stats] def startOfNextMinute: Time =
    Time.fromSeconds(Time.now.inMinutes * 60) + 1.minute

  private val log = Logger.get()
}

class JsonExporter(
    registry: Metrics,
    timer: Timer)
  extends Service[Request, Response] { self =>

  import JsonExporter._

  def this(registry: Metrics) = this(registry, DefaultTimer.twitter)

  private[this] val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  private[this] val writer = mapper.writer
  private[this] val prettyWriter = mapper.writer(new DefaultPrettyPrinter)

  lazy val statsFilterRegex: Option[Regex] = statsFilter.get.flatMap { regexesString =>
    mkRegex(regexesString)
  }

  private[this] val registryLoaded = new AtomicBoolean(false)

  // thread-safety provided by synchronization on `this`
  private[this] var deltas: Option[CounterDeltas] = None

  def apply(request: Request): Future[Response] = {
    if (registryLoaded.compareAndSet(false, true)) {
      GlobalRegistry.get.put(
        Seq("stats", "commons_metrics", "counters_latched"),
        useCounterDeltas().toString)
    }

    val response = Response()
    response.contentType = MediaType.Json

    val params = new RequestParamMap(request)
    val pretty = readBooleanParam(params, name = "pretty", default = false)
    val filtered = readBooleanParam(params, name = "filtered", default = false)
    val counterDeltasOn = {
      val vals = params.getAll("period")
      if (vals.isEmpty) {
        false
      } else {
        if (vals.exists(_ == "60")) true else {
          log.warning(s"${getClass.getName} request ignored due to unsupported period: '${vals.mkString(",")}'")
          false
        }
      }
    }

    val asJson = json(pretty, filtered, counterDeltasOn)
    response.content = Buf.Utf8(asJson)
    Future.value(response)
  }

  // package protected for testing
  private[stats] def readBooleanParam(
    params: RequestParamMap,
    name: String,
    default: Boolean
  ): Boolean = {
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
          ds.update(registry.sampleCounters())
        }
        deltas.get
    }
  }

  def json(
    pretty: Boolean,
    filtered: Boolean,
    counterDeltasOn: Boolean = false
  ): String = {
    val gauges = registry.sampleGauges().asScala
    val histos = registry.sampleHistograms().asScala
    val counters = if (counterDeltasOn && useCounterDeltas()) {
      getOrRegisterLatchedStats().deltas
    } else {
      registry.sampleCounters().asScala
    }
    val values = SampledValues(gauges, counters, histos)

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

  def mkRegex(regexesString: String): Option[Regex] = {
    regexesString.split(",") match {
      case Array("") => None
      case regexes => Some(regexes.mkString("(", ")|(", ")").r)
    }
  }

  def filterSample(sample: collection.Map[String, Number]): collection.Map[String, Number] = {
    statsFilterRegex match {
      case Some(regex) => sample.filterKeys(!regex.pattern.matcher(_).matches)
      case None => sample
    }
  }

}
