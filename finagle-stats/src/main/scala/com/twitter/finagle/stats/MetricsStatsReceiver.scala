package com.twitter.finagle.stats

import com.twitter.common.metrics.{Histogram, HistogramInterface, AbstractGauge, Metrics}
import com.twitter.finagle.http.HttpMuxHandler
import com.twitter.io.{Reader, Buf}
import com.twitter.util.{Time, Return, Throw, Try}
import com.twitter.util.events.{Event, Sink}
import java.util.concurrent.ConcurrentHashMap

private object Json {
  import com.fasterxml.jackson.core.`type`.TypeReference
  import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  import java.lang.reflect.{Type, ParameterizedType}

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

object MetricsStatsReceiver {
  val defaultRegistry = Metrics.root()
  private[this] val _defaultHostRegistry = Metrics.createDetached()
  val defaultHostRegistry = _defaultHostRegistry
  private def defaultFactory(name: String): HistogramInterface = new Histogram(name)

  /**
   * The [[com.twitter.util.events.Event.Type Event.Type]] for counter increment events.
   */
  val CounterIncr: Event.Type = {
    case class Data(name: String, value: Long)
    case class Envelope(id: String, when: Long, data: Data)

    new Event.Type {
      val id = "CounterIncr"

      def serialize(event: Event) = event match {
        case Event(etype, when, value, name: String, _) if etype eq this =>
          val env = Envelope(id, when.inMilliseconds, Data(name, value))
          Try(Buf.Utf8(Json.serialize(env)))

        case _ =>
          Throw(new IllegalArgumentException("unknown format: " + event))
      }

      def deserialize(buf: Buf) = for {
        (idd, when, data) <- Buf.Utf8.unapply(buf) match {
          case None => Throw(new IllegalArgumentException("unknown format"))
          case Some(str) => Try {
            val env = Json.deserialize[Envelope](str)
            (env.id, Time.fromMilliseconds(env.when), env.data)
          }
        }
        if idd == id
      } yield Event(this, when, objectVal = data.name, longVal = data.value)
    }
  }

  /**
   * The [[com.twitter.util.events.Event.Type Event.Type]] for stat add events.
   */
  val StatAdd: Event.Type = {
    case class Data(name: String, delta: Long)
    case class Envelope(id: String, when: Long, data: Data)

    new Event.Type {
      val id = "StatAdd"

      def serialize(event: Event) = event match {
        case Event(etype, when, delta, name: String, _) if etype eq this =>
          val env = Envelope(id, when.inMilliseconds, Data(name, delta))
          Try(Buf.Utf8(Json.serialize(env)))

        case _ =>
          Throw(new IllegalArgumentException("unknown format: " + event))
      }

      def deserialize(buf: Buf) = for {
        (idd, when, data) <- Buf.Utf8.unapply(buf) match {
          case None => Throw(new IllegalArgumentException("unknown format"))
          case Some(str) => Try {
            val env = Json.deserialize[Envelope](str)
            (env.id, Time.fromMilliseconds(env.when), env.data)
          }
        }
        if idd == id
      } yield Event(this, when, objectVal = data.name, longVal = data.delta)
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
            sink.event(CounterIncr, objectVal = metricsCounter.getName(), longVal = delta)
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
        stat = new Stat {
          val histogram = histogramFactory(format(names))
          registry.registerHistogram(histogram)
          def add(value: Float): Unit = {
            val asLong = value.toLong
            histogram.add(asLong)
            sink.event(StatAdd, objectVal = histogram.getName(), longVal = asLong)
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
