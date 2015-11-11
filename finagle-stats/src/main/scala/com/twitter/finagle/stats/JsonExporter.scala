package com.twitter.finagle.stats

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.app.GlobalFlag
import com.twitter.common.metrics.Metrics
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{MediaType, RequestParamMap, Response, Request, Status}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.util.registry.GlobalRegistry
import java.util.concurrent.atomic.AtomicBoolean
import java.io.{IOException, File}
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.immutable
import scala.io.Source
import scala.util.matching.Regex

/**
 * Blacklist of regex, comma-separated. Comma is a reserved character and
 * cannot be used. Used with regexes from statsFilterFile.
 *
 * See http://www.scala-lang.org/api/current/#scala.util.matching.Regex
 */
object statsFilter extends GlobalFlag[String](
  "",
  "Comma-separated regexes that indicate which metrics to filter out")


/**
 * Blacklist of regex, comma-separated. Comma is a reserved character and
 * cannot be used. Used with regexes from statsFilter.
 *
 * See http://www.scala-lang.org/api/current/#scala.util.matching.Regex
 */
object statsFilterFile extends GlobalFlag[File](
  "File of newline separated regexes that indicate which metrics to filter out")

object useCounterDeltas extends GlobalFlag[Boolean](
  false,
  "Return deltas for counters instead of absolute values. " +
    "Provides compatibility with the behavior from 'Ostrich'"
)

object JsonExporter {

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

  lazy val statsFilterRegex: Option[Regex] = {
    val regexesFromFile = statsFilterFile.get.toSeq.flatMap { fileName =>
      try {
        Source.fromFile(fileName.toString).getLines()
      } catch {
        case e: IOException =>
          log.error(e, "Unable to read statsFilterFile: %s", fileName)
          throw e
      }
    }
    val regexesFromFlag = statsFilter.get.toSeq.flatMap(_.split(","))
    val regexes: Seq[String] = regexesFromFlag ++ regexesFromFile
    mkRegex(regexes)
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
    val gauges = try registry.sampleGauges().asScala catch {
      case NonFatal(e) =>
        // because gauges run arbitrary user code, we want to protect ourselves here.
        // while the underlying registry should protect against individual misbehaving
        // gauges, an extra level of belt-and-suspenders seemed worthwhile.
        log.error(e, "exception while collecting gauges")
        Map.empty[String, Number]
    }
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

  private[this] def mkRegex(regexes: Seq[String]): Option[Regex] = {
    if (regexes.isEmpty) {
      None
    } else {
      Some(regexes.mkString("(", ")|(", ")").r)
    }
  }

  def mkRegex(regexesString: String): Option[Regex] = {
    regexesString.split(",") match {
      case Array("") => None
      case regexes => mkRegex(regexes)
    }
  }

  def filterSample(sample: collection.Map[String, Number]): collection.Map[String, Number] = {
    statsFilterRegex match {
      case Some(regex) => sample.filterKeys(!regex.pattern.matcher(_).matches)
      case None => sample
    }
  }

}
