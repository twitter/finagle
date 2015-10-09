package com.twitter.finagle.zipkin.thrift

import com.twitter.finagle.NoStacktrace
import com.twitter.finagle.stats.{DefaultStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.{TraceId, Record, Tracer, Annotation, Trace}
import com.twitter.finagle.zipkin.{host => Host, initialSampleRate => sampleRateFlag}
import com.twitter.io.Buf
import com.twitter.util.events.{Event, Sink}
import com.twitter.util.{Time, Throw, Try}

private object Json {
  import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonInclude}
  import com.fasterxml.jackson.core.`type`.TypeReference
  import com.fasterxml.jackson.databind.{ObjectMapper, JavaType, JsonNode}
  import com.fasterxml.jackson.databind.annotation.JsonDeserialize
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  import java.lang.reflect.{Type, ParameterizedType}

  // Note: This type is a just a convenience for deserialization in other
  // other Event.Type constructions, but we actually require it for Trace
  // because we're using Jackson's default typing mechanism for Annotation.
  // If we use a Map, somewhere in Jackson's type resolution the type of
  // Annotation is forgotten, and it is passed into the type resolver as an
  // Object. Defining this Envelope preserves the type information.
  @JsonInclude(JsonInclude.Include.NON_NULL)
  case class Envelope(
      id: String,
      when: Long,
      // We require an annotation here, because for small numbers, this gets
      // deserialized with a runtime type of int.
      // See: https://github.com/FasterXML/jackson-module-scala/issues/106.
      @JsonDeserialize(contentAs=classOf[java.lang.Long]) traceId: Option[Long],
      @JsonDeserialize(contentAs=classOf[java.lang.Long]) spanId: Option[Long],
      data: Annotation)

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // Configures the mapper to include class information for Annotation.
  object TypeResolverBuilder
    extends ObjectMapper.DefaultTypeResolverBuilder(ObjectMapper.DefaultTyping.NON_FINAL) {
    override def useForType(typ: JavaType) =
      // Note: getRawClass would be an Object if not for `Envelope`.
      typ.getRawClass == classOf[Annotation]
  }

  mapper.setDefaultTyping(
    TypeResolverBuilder
      .init(JsonTypeInfo.Id.CLASS, null)
      .inclusion(JsonTypeInfo.As.WRAPPER_ARRAY))

  def serialize(o: AnyRef): String = mapper.writeValueAsString(o)

  def deserialize[T: Manifest](value: String): T =
    mapper.readValue(value, typeReference[T])

  def deserialize[T: Manifest](node: JsonNode): T =
    mapper.readValue(node.traverse, typeReference[T])

  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }

  private[this] def typeFromManifest(m: Manifest[_]): Type =
    if (m.typeArguments.isEmpty) m.runtimeClass else new ParameterizedType {
      def getRawType = m.runtimeClass
      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
      def getOwnerType = null
    }
}

object ZipkinTracer {

  lazy val default: Tracer = mk()

  /**
   * The [[com.twitter.util.events.Event.Type Event.Type]] for trace events.
   */
  val Trace: Event.Type = {

    new Event.Type {
      val id = "Trace"

      def serialize(event: Event) = event match {
        case Event(etype, _, _, _: Annotation.BinaryAnnotation, _, _, _) if etype eq this =>
          Throw(new IllegalArgumentException("unsupported format: " + event) with NoStacktrace)

        case Event(etype, when, _, ann: Annotation, _, tid, sid) if etype eq this =>
          val (t, s) = serializeTrace(tid, sid)
          val data = Json.Envelope(id, when.inMilliseconds, t, s, ann)
          Try(Buf.Utf8(Json.serialize(data)))

        case _ =>
          Throw(new IllegalArgumentException("unknown format: " + event))
      }

      def deserialize(buf: Buf) = for {
        env <- Buf.Utf8.unapply(buf) match {
          case None => Throw(new IllegalArgumentException("unknown format"))
          case Some(str) => Try(Json.deserialize[Json.Envelope](str))
        }
        if env.id == id
      } yield {
        val when = Time.fromMilliseconds(env.when)
        // This line fails without the JsonDeserialize annotation in Envelope.
        val tid = env.traceId.getOrElse(Event.NoTraceId)
        val sid = env.spanId.getOrElse(Event.NoSpanId)
        Event(this, when, objectVal = env.data, traceIdVal = tid, spanIdVal = sid)
      }
    }
  }

  /**
   * @param scribeHost Host to send trace data to
   * @param scribePort Port to send trace data to
   * @param statsReceiver Where to log information about tracing success/failures
   * @param sampleRate How much data to collect. Default sample rate 0.1%. Max is 1, min 0.
   */
  @deprecated("Use mk() instead", "6.1.0")
  def apply(
    scribeHost: String = Host().getHostName,
    scribePort: Int = Host().getPort,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sampleRate: Float = Sampler.DefaultSampleRate
  ): Tracer.Factory = () => mk(scribeHost, scribePort, statsReceiver, sampleRate)

  /**
   * @param host Host to send trace data to
   * @param port Port to send trace data to
   * @param statsReceiver Where to log information about tracing success/failures
   * @param sampleRate How much data to collect. Default sample rate 0.1%. Max is 1, min 0.
   */
  def mk(
    host: String = Host().getHostName,
    port: Int = Host().getPort,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    sampleRate: Float = Sampler.DefaultSampleRate
  ): Tracer =
    new ZipkinTracer(
      RawZipkinTracer(host, port, statsReceiver),
      sampleRate)

  /**
   * Util method since named parameters can't be called from Java
   * @param sr stats receiver to send successes/failures to
   */
  @deprecated("Use mk() instead", "6.1.0")
  def apply(sr: StatsReceiver): Tracer.Factory = () =>
    mk(Host().getHostName, Host().getPort, sr, Sampler.DefaultSampleRate)

  /**
   * Util method since named parameters can't be called from Java
   * @param statsReceiver stats receiver to send successes/failures to
   */
  def mk(statsReceiver: StatsReceiver): Tracer =
    mk(Host().getHostName, Host().getPort, statsReceiver, Sampler.DefaultSampleRate)
}

/**
 * Tracer that supports sampling. Will pass through a subset of the records.
 * @param underlyingTracer Underlying tracer that accumulates the traces and sends off
 *          to the collector.
 * @param initialSampleRate Start off with this sample rate. Can be changed later.
 * @param sink where to send sampled trace events to.
 */
class SamplingTracer(
    underlyingTracer: Tracer,
    initialSampleRate: Float,
    sink: Sink)
  extends Tracer
{

  /**
   * Tracer that supports sampling. Will pass through a subset of the records.
   * @param underlyingTracer Underlying tracer that accumulates the traces and sends off
   *          to the collector.
   * @param initialSampleRate Start off with this sample rate. Can be changed later.
   */
  def this(underlyingTracer: Tracer, initialSampleRate: Float) =
    this(underlyingTracer, initialSampleRate, Sink.default)

  /**
   * Tracer that supports sampling. Will pass through a subset of the records.
   */
  def this() = this(
    RawZipkinTracer(Host().getHostName, Host().getPort, DefaultStatsReceiver.scope("zipkin")),
    sampleRateFlag())

  private[this] val sampler = new Sampler
  setSampleRate(initialSampleRate)

  def sampleTrace(traceId: TraceId): Option[Boolean] = sampler.sampleTrace(traceId)

  def setSampleRate(sampleRate: Float): Unit = sampler.setSampleRate(sampleRate)
  def getSampleRate: Float = sampler.sampleRate

  def record(record: Record) {
    if (sampler.sampleRecord(record)) {
      underlyingTracer.record(record)
      if (sink.recording) {
        if (Trace.hasId) {
          val traceId = Trace.id
          sink.event(ZipkinTracer.Trace, objectVal = record.annotation,
            traceIdVal = traceId.traceId.self, spanIdVal = traceId.spanId.self)
        } else {
          sink.event(ZipkinTracer.Trace, objectVal = record.annotation)
        }
      }
    }
  }
}

class ZipkinTracer(tracer: RawZipkinTracer, initialRate: Float)
  extends SamplingTracer(tracer, initialRate)
