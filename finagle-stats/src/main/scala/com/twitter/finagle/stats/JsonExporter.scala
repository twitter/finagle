package com.twitter.finagle.stats

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.http.{MediaType, ParamMap, Request, Response}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.tunable.Tunable
import java.util.concurrent.atomic.AtomicBoolean
import java.io.{File, IOException}
import java.util.regex.Pattern
import scala.annotation.switch
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.immutable
import scala.io.{Codec, Source}
import scala.util.matching.Regex
import scala.util.control.NonFatal

/**
 * Blacklist of regex, comma-separated. Comma is a reserved character and
 * cannot be used. Used with regexes from statsFilterFile.
 *
 * See http://www.scala-lang.org/api/current/#scala.util.matching.Regex
 */
object statsFilter
    extends GlobalFlag[String](
      "",
      "Comma-separated regexes that indicate which metrics to filter out"
    )

/**
 * Comma-separated blacklist of files. Each file may have multiple filters,
 * separated by new lines. Used with regexes from statsFilter.
 *
 * See http://www.scala-lang.org/api/current/#scala.util.matching.Regex
 */
object statsFilterFile
    extends GlobalFlag[Set[File]](
      Set.empty[File],
      "Comma separated files of newline separated regexes that indicate which metrics to filter out"
    )

object useCounterDeltas
    extends GlobalFlag[Boolean](
      false,
      "Return deltas for counters instead of absolute values. " +
        "Provides compatibility with the behavior from 'Ostrich'"
    )

object JsonExporter {

  private[stats] def startOfNextMinute: Time =
    Time.fromSeconds(Time.now.inMinutes * 60) + 1.minute

  private val log: Logger = Logger.get()

  /**
   * Merges individual regular expressions (represented as a sequence of strings) into
   * a [[Regex]] instance that is matches as long as one of these expressions is matched.
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

  /**
   * A simplified (only *-wildcards are supported), possibly comma-separated list of Glob
   * expressions.
   *
   * @see https://en.wikipedia.org/wiki/Glob_(programming)
   */
  private[stats] def commaSeparatedGlob(glob: String): Option[Pattern] =
    if (glob.isEmpty) None
    else {
      var i = 0
      // We expand the resulting string to fit up to 8 regex characters w/o
      // resizing the string builder.
      val result = new StringBuilder(glob.length + 8)
      while (i < glob.length) {
        (glob.charAt(i): @switch) match {
          case '[' => result.append("\\[")
          case ']' => result.append("\\]")
          case '|' => result.append("\\|")
          case '^' => result.append("\\^")
          case '$' => result.append("\\$")
          case '.' => result.append("\\.")
          case '?' => result.append("\\?")
          case '+' => result.append("\\+")
          case '(' => result.append("\\(")
          case ')' => result.append("\\)")
          case '{' => result.append("\\{")
          case '}' => result.append("\\}")
          case '*' => result.append(".*")
          case c => result.append(c)
        }
        i += 1
      }

      commaSeparatedRegex(result.toString).map(_.pattern)
    }
}

/**
 * A Finagle HTTP service that exports [[Metrics]] (available via [[MetricsView]])
 * in a JSON format.
 *
 * This service respects metrics [[Verbosity verbosity levels]]: it doesn't export "debug"
 * (i.e., [[Verbosity.Debug]]) metrics unless they are whitelisted via the comma-separated
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

  lazy val statsFilterRegex: Option[Regex] = {
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
    mergedRegex(regexesFromFlag ++ regexesFromFile)
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
  private[stats] def readBooleanParam(
    params: ParamMap,
    name: String,
    default: Boolean
  ): Boolean = {
    val vals = params.getAll(name)
    if (vals.nonEmpty)
      vals.exists { v =>
        v == "1" || v == "true"
      } else
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

  def json(
    pretty: Boolean,
    filtered: Boolean,
    counterDeltasOn: Boolean = false
  ): String = {
    val gauges = try metrics.gauges.asScala
    catch {
      case NonFatal(e) =>
        // because gauges run arbitrary user code, we want to protect ourselves here.
        // while the underlying registry should protect against individual misbehaving
        // gauges, an extra level of belt-and-suspenders seemed worthwhile.
        log.error(e, "exception while collecting gauges")
        Map.empty[String, Number]
    }
    val histos = metrics.histograms.asScala
    val counters = if (counterDeltasOn && useCounterDeltas()) {
      getOrRegisterLatchedStats().deltas
    } else {
      metrics.counters.asScala
    }

    // Converting a *-wildcard expression into a regular expression so we can match on it.
    val verbosePatten = verbose().flatMap(commaSeparatedGlob)

    // We have to blacklist debug metrics before we apply formatting, which may change
    // the names.
    val values = SampledValues(
      blacklistDebugSample(gauges, verbosePatten),
      blacklistDebugSample(counters, verbosePatten),
      blacklistDebugSample(histos, verbosePatten)
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

  def filterSample(sample: collection.Map[String, Number]): collection.Map[String, Number] =
    statsFilterRegex match {
      case Some(regex) => sample.filterKeys(!regex.pattern.matcher(_).matches)
      case None => sample
    }

  private final def blacklistDebugSample[A](
    sample: collection.Map[String, A],
    verbose: Option[Pattern]
  ): collection.Map[String, A] = verbose match {
    case Some(pattern) =>
      sample.filterKeys(
        name => metrics.verbosity.get(name) != Verbosity.Debug || pattern.matcher(name).matches
      )

    case None =>
      sample.filterKeys(name => metrics.verbosity.get(name) != Verbosity.Debug)
  }
}
