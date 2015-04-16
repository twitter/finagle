package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import com.twitter.common.metrics.{HistogramInterface, AbstractGauge, Metrics}
import com.twitter.finagle.httpx.HttpMuxHandler
import com.twitter.finagle.tracing.Trace
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.events.{Event, Sink}
import com.twitter.util.{Time, Throw, Try}
import java.util.concurrent.ConcurrentHashMap

private object Json {
  import com.fasterxml.jackson.annotation.JsonInclude
  import com.fasterxml.jackson.core.`type`.TypeReference
  import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
  import com.fasterxml.jackson.databind.annotation.JsonDeserialize
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  import java.lang.reflect.{Type, ParameterizedType}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  case class Envelope[A](
      id: String,
      when: Long,
      // We require an annotation here, because for small numbers, this gets
      // deserialized with a runtime type of int.
      // See: https://github.com/FasterXML/jackson-module-scala/issues/106.
      @JsonDeserialize(contentAs = classOf[java.lang.Long]) traceId: Option[Long],
      @JsonDeserialize(contentAs = classOf[java.lang.Long]) spanId: Option[Long],
      data: A)

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def serialize(o: AnyRef): String = mapper.writeValueAsString(o)

  def deserialize[T: Manifest](value: String): T =
    mapper.readValue(value, typeReference[T])

  def deserialize[T: Manifest](node: JsonNode): T =
    mapper.readValue(node.traverse, typeReference[T])

  private def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }

  private def typeFromManifest(m: Manifest[_]): Type =
    if (m.typeArguments.isEmpty) m.runtimeClass else new ParameterizedType {
      def getRawType = m.runtimeClass
      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
      def getOwnerType = null
    }
}

// The ordering issue is that LoadService is run early in the startup
// lifecycle, typically before Flags are loaded. By using a system
// property you can avoid that brittleness.
object debugLoggedStatNames extends GlobalFlag[Set[String]](
    Set.empty,
    "Comma separated stat names for logging observed values" +
      " (set via a -D system property to avoid load ordering issues)")

object MetricsStatsReceiver {
  val defaultRegistry = Metrics.root()
  private[this] val _defaultHostRegistry = Metrics.createDetached()
  val defaultHostRegistry = _defaultHostRegistry

  private[this] val log = Logger.get()

  private def defaultFactory(name: String): HistogramInterface =
    new MetricsBucketedHistogram(name)

  private[this] case class CounterIncrData(name: String, value: Long)
  private[this] case class StatAddData(name: String, delta: Long)

  /**
   * The [[com.twitter.util.events.Event.Type Event.Type]] for counter increment events.
   */
  val CounterIncr: Event.Type = {
    new Event.Type {
      val id = "CounterIncr"

      def serialize(event: Event) = event match {
        case Event(etype, when, value, name: String, _, tid, sid) if etype eq this =>
          val (t, s) = serializeTrace(tid, sid)
          val env = Json.Envelope(id, when.inMilliseconds, t, s, CounterIncrData(name, value))
          Try(Buf.Utf8(Json.serialize(env)))

        case _ =>
          Throw(new IllegalArgumentException("unknown format: " + event))
      }

      def deserialize(buf: Buf) = for {
        env <- Buf.Utf8.unapply(buf) match {
          case None => Throw(new IllegalArgumentException("unknown format"))
          case Some(str) => Try(Json.deserialize[Json.Envelope[CounterIncrData]](str))
        }
        if env.id == id
      } yield {
        val when = Time.fromMilliseconds(env.when)
        // This line fails without the JsonDeserialize annotation in Envelope.
        val tid = env.traceId.getOrElse(Event.NoTraceId)
        val sid = env.spanId.getOrElse(Event.NoSpanId)
        Event(this, when, longVal = env.data.value,
          objectVal = env.data.name, traceIdVal = tid, spanIdVal = sid)
      }
    }
  }

  /**
   * The [[com.twitter.util.events.Event.Type Event.Type]] for stat add events.
   */
  val StatAdd: Event.Type = {
    new Event.Type {
      val id = "StatAdd"

      def serialize(event: Event) = event match {
        case Event(etype, when, delta, name: String, _, tid, sid) if etype eq this =>
          val (t, s) = serializeTrace(tid, sid)
          val env = Json.Envelope(id, when.inMilliseconds, t, s, StatAddData(name, delta))
          Try(Buf.Utf8(Json.serialize(env)))

        case _ =>
          Throw(new IllegalArgumentException("unknown format: " + event))
      }

      def deserialize(buf: Buf) = for {
        env <- Buf.Utf8.unapply(buf) match {
          case None => Throw(new IllegalArgumentException("unknown format"))
          case Some(str) => Try(Json.deserialize[Json.Envelope[StatAddData]](str))
        }
        if env.id == id
      } yield {
        val when = Time.fromMilliseconds(env.when)
        // This line fails without the JsonDeserialize annotation in Envelope.
        val tid = env.traceId.getOrElse(Event.NoTraceId)
        val sid = env.spanId.getOrElse(Event.NoSpanId)
        Event(this, when, longVal = env.data.delta,
          objectVal = env.data.name, traceIdVal = tid, spanIdVal = sid)
      }
    }
  }
}

/**
 * This implementation of StatsReceiver uses the [[com.twitter.common.metrics]] library under
 * the hood.
 *
 * Note: Histogram uses [[com.twitter.common.stats.WindowedApproxHistogram]] under the hood.
 * It is (by default) configured to store events in a 80 seconds moving window, reporting
 * metrics on the first 60 seconds. It means that when you add a value, you need to wait at most
 * 20 seconds before this value will be aggregated in the exported metrics.
 */
class MetricsStatsReceiver(
  val registry: Metrics,
  sink: Sink,
  histogramFactory: String => HistogramInterface
) extends StatsReceiverWithCumulativeGauges {
  import MetricsStatsReceiver._

  def this(registry: Metrics, sink: Sink) = this(registry, sink, MetricsStatsReceiver.defaultFactory)
  def this(registry: Metrics) = this(registry, Sink.default)
  def this() = this(MetricsStatsReceiver.defaultRegistry)

  val repr = this

  // Use for backward compatibility with ostrich caching behavior
  private[this] val counters = new ConcurrentHashMap[Seq[String], Counter]
  private[this] val stats = new ConcurrentHashMap[Seq[String], Stat]

  private[this] val log = Logger.get()

  private[this] val loggedStats: Set[String] = debugLoggedStatNames()

  /**
   * Create and register a counter inside the underlying Metrics library
   */
  def counter(names: String*): Counter = {
    var counter = counters.get(names)
    if (counter == null) counters.synchronized {
      counter = counters.get(names)
      if (counter == null) {
        counter = new Counter {
          val metricsCounter = registry.createCounter(format(names))
          def incr(delta: Int): Unit = {
            metricsCounter.add(delta)
            if (Trace.hasId) {
              sink.event(CounterIncr, objectVal = metricsCounter.getName(), longVal = delta,
                traceIdVal = Trace.id.traceId.self, spanIdVal = Trace.id.spanId.self)
            } else {
              sink.event(CounterIncr, objectVal = metricsCounter.getName(), longVal = delta)
            }
          }
        }
        counters.put(names, counter)
      }
    }
    counter
  }

  /**
   * Create and register a stat (histogram) inside the underlying Metrics library
   */
  def stat(names: String*): Stat = {
    var stat = stats.get(names)
    if (stat == null) stats.synchronized {
      stat = stats.get(names)
      if (stat == null) {
        val doLog = loggedStats.contains(format(names))
        stat = new Stat {
          val histogram = histogramFactory(format(names))
          registry.registerHistogram(histogram)
          def add(value: Float): Unit = {
            if (doLog) log.info(s"Stat ${histogram.getName()} observed $value")
            val asLong = value.toLong
            histogram.add(asLong)
            if (Trace.hasId) {
              sink.event(StatAdd, objectVal = histogram.getName(), longVal = asLong,
                traceIdVal = Trace.id.traceId.self, spanIdVal = Trace.id.spanId.self)
            } else {
              sink.event(StatAdd, objectVal = histogram.getName(), longVal = asLong)
            }
          }
        }
        stats.put(names, stat)
      }
    }
    stat
  }

  protected[this] def registerGauge(names: Seq[String], f: => Float) {
    val gauge = new AbstractGauge[java.lang.Double](format(names)) {
      override def read = new java.lang.Double(f)
    }
    registry.register(gauge)
  }

  protected[this] def deregisterGauge(names: Seq[String]) {
    registry.unregister(format(names))
  }

  private[this] def format(names: Seq[String]) = names.mkString("/")
}

class MetricsExporter(val registry: Metrics)
  extends JsonExporter(registry)
  with HttpMuxHandler
  with MetricsRegistry
{
  def this() = this(MetricsStatsReceiver.defaultRegistry)
  val pattern = "/admin/metrics.json"
}
