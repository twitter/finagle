package com.twitter.finagle.b3.thrift

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.scribe.{ResultCode, LogEntry, scribe}
import org.apache.thrift.transport.TIOStreamTransport

import java.net.InetSocketAddress
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.twitter.conversions.time._
import com.twitter.util.Base64StringEncoder
import com.twitter.finagle.tracing
import com.twitter.finagle.tracing._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, thrift}

import collection.mutable.{ArrayBuffer, WeakHashMap}
import scala.collection.JavaConversions._

object BigBrotherBirdTracer {

  // to make sure we only create one instance of the tracer
  // per host and port
  private val map = WeakHashMap[String, BigBrotherBirdTracer]()

  def apply(scribeHost: String = "localhost",
            scribePort: Int = 1463,
            statsReceiver: StatsReceiver): BigBrotherBirdTracer = {

    map.getOrElseUpdate(scribeHost + ":" + scribePort, {
        val transport = ClientBuilder()
          .hosts(new InetSocketAddress(scribeHost, scribePort))
          .codec(ThriftClientFramedCodec())
          .hostConnectionLimit(5)
          .build()
        val client = new scribe.ServiceToClient(transport, new TBinaryProtocol.Factory())

        new BigBrotherBirdTracer(client, statsReceiver)
    })
  }

}

/**
 * Receives the Finagle generated traces, samples them
 * and sends off the survivors to BigBrotherBird via scribe.
 * @param client The Thrift client used to send traces to scribe
 * @param statsReceiver We generate stats to keep track of traces sent, failures and so on
 */
private[thrift] class BigBrotherBirdTracer(
  client: scribe.ServiceToClient,
  statsReceiver: StatsReceiver = NullStatsReceiver) extends Tracer
{
  private[this] val protocolFactory = new TBinaryProtocol.Factory()
  private[this] val TraceCategory = "b3" // scribe category

  // this sends off spans after the deadline is hit, no matter if it ended naturally or not.
  private[this] val spanMap = new DeadlineSpanMap(this, 120.seconds)
  private[this] var sampleRate = 0.001f // default sample rate 0.1%. Max is 1, min 0.

  /**
   * Set the sample rate.
   *
   * How much to let through? For everything, use 1 = 100.00%
   * Default is 0.001 = 0.1% (let one in a 1000nd pass)
   */
  def setSampleRate(sr: Float): BigBrotherBirdTracer = {
    if (sr < 0 || sr > 1) {
      throw new IllegalArgumentException("Sample rate not within the valid range of 0-1, was " + sr)
    }
    sampleRate = sr
    this
  }

  /**
   * Serialize the spans, base64 encode and shove it all in a list.
   */
  private def createLogEntries(span: Span): ArrayBuffer[LogEntry] = {
    var msgs = new ArrayBuffer[LogEntry]()

    try {
      val s = span.toThrift
      val baos = new ByteArrayOutputStream
      s.write(protocolFactory.getProtocol(new TIOStreamTransport(baos)))
      val serializedBase64Span = Base64StringEncoder.encode(baos.toByteArray)
      msgs = msgs :+ new LogEntry().setCategory(TraceCategory).setMessage(serializedBase64Span)
    } catch {
      case e => statsReceiver.counter("create_log_entries_%s".format(e.toString)).incr()
    }

    msgs
  }

  /**
   * Log the span data via Scribe.
   */
  def logSpan(span: Span) {
    client.Log(createLogEntries(span)) onSuccess {
      case ResultCode.OK => statsReceiver.counter("log_span_ok").incr()
      case ResultCode.TRY_LATER => statsReceiver.counter("log_span_try_later").incr()
      case _ => () /* ignore */
    } onFailure {
      case e => statsReceiver.counter("log_span_%s".format(e.toString)).incr()
    }
  }

  /**
   * Should we drop this particular trace or send it on to Scribe?
   * True means drop.
   * False means keep.
   */
  def sampleTrace(traceId: TraceId): Boolean = {
    math.abs(traceId.traceId.toLong) % 10000 >= sampleRate * 10000
  }

  /**
   * Mutate the Span with whatever new info we have.
   * If we see an "end" annotation we remove the span and send it off.
   */
  protected def mutate(traceId: TraceId)(f: Span => Span) {
    val span = spanMap.update(traceId)(f)

    // if either two "end annotations" exists we send off the span
    if (span.annotations.exists { a =>
      a.value.equals(thrift.Constants.CLIENT_RECV) || a.value.equals(thrift.Constants.SERVER_SEND)
    }) {
      spanMap.remove(traceId)
      logSpan(span)
    }
  }

  def record(record: Record) {
    // if this trace is marked as sampled we just throw away all the records
    if (!record.traceId.sampled) {
      record.annotation match {
        case tracing.Annotation.ClientSend()   =>
          annotate(record, thrift.Constants.CLIENT_SEND)
        case tracing.Annotation.ClientRecv()   =>
          annotate(record, thrift.Constants.CLIENT_RECV)
        case tracing.Annotation.ServerSend()   =>
          annotate(record, thrift.Constants.SERVER_SEND)
        case tracing.Annotation.ServerRecv()   =>
          annotate(record, thrift.Constants.SERVER_RECV)
        case tracing.Annotation.Message(value) =>
          annotate(record, value)
        case tracing.Annotation.Rpcname(service: String, rpc: String) =>
          mutate(record.traceId) { span =>
            span.copy(_name = Some(rpc), _serviceName = Some(service))
          }
        case tracing.Annotation.BinaryAnnotation(key: String, value: ByteBuffer) =>
          mutate(record.traceId) { span =>
            span.copy(bAnnotations = span.bAnnotations + (key -> value))
          }
        case tracing.Annotation.ClientAddr(ia: InetSocketAddress) =>
          setEndpoint(record, ia)
        case tracing.Annotation.ServerAddr(ia: InetSocketAddress) =>
          setEndpoint(record, ia)
      }
    }
  }

  /**
   * Sets the endpoint in the span for any future annotations. Also
   * sets the endpoint in any previous annotations.
   */
  protected def setEndpoint(record: Record, ia: InetSocketAddress) {
    mutate(record.traceId) { span =>
      val endpoint = Endpoint.fromSocketAddress(ia).boundEndpoint
      span.copy(_endpoint = Some(endpoint),
        annotations = span.annotations map { a => B3Annotation(a.timestamp, a.value, endpoint)})
    }
  }

  /**
   * Add this record as a time based annotation.
   */
  protected def annotate(record: Record, value: String) = {
    mutate(record.traceId) { span =>
      span.copy(annotations = span.annotations ++ Seq(
        B3Annotation(record.timestamp, value, span.endpoint)))
    }
  }

}