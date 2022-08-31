package com.twitter.finagle.zipkin.core

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.thrift.thrift
import com.twitter.finagle.tracing
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import java.net.InetSocketAddress
import java.nio.ByteBuffer

private object RawZipkinTracer {
  val ErrorAnnotation = "%s: %s" // annotation: errorMessage
}

/**
 * Receives the Finagle generated traces and sends them off to Zipkin
 * @param timer A Timer used for timing out spans in the [[DeadlineSpanMap]]
 */
// Not private, so that this can be extended to support other transports, such as Kafka.
// See https://github.com/openzipkin/zipkin-finagle
abstract class RawZipkinTracer(timer: Timer = DefaultTimer) extends Tracer {
  import RawZipkinTracer._

  // this sends off spans after the deadline is hit, no matter if it ended naturally or not.
  private[this] val spanMap: DeadlineSpanMap =
    new DeadlineSpanMap(sendSpans, 120.seconds, timer)

  /* exposed for testing */
  protected[finagle] def flush(): Future[Unit] = spanMap.flush()

  /**
   * Always sample the request.
   */
  def sampleTrace(traceId: TraceId): Option[Boolean] = Tracer.SomeTrue

  // technically we're not using the sample rate, but it's effectively 100%
  def getSampleRate: Float = 1f

  override def isActivelyTracing(traceId: TraceId): Boolean = true

  /**
   * Mutate the Span with whatever new info we have.
   * If we see an "end" annotation we remove the span and send it off.
   */
  protected def mutate(traceId: TraceId)(f: MutableSpan => Unit): Unit = {
    spanMap.update(traceId)(f)
  }

  private[this] val TrueBB: ByteBuffer = ByteBuffer.wrap(Array[Byte](1))
  private[this] val FalseBB: ByteBuffer = ByteBuffer.wrap(Array[Byte](0))

  def record(record: Record): Unit = {
    record.annotation match {
      case tracing.Annotation.WireSend =>
        annotate(record, thrift.Constants.WIRE_SEND)
      case tracing.Annotation.WireRecv =>
        annotate(record, thrift.Constants.WIRE_RECV)
      case tracing.Annotation.WireRecvError(error: String) =>
        annotate(record, ErrorAnnotation.format(thrift.Constants.WIRE_RECV_ERROR, error))
      case tracing.Annotation.ClientSend =>
        annotate(record, thrift.Constants.CLIENT_SEND)
      case tracing.Annotation.ClientRecv =>
        annotate(record, thrift.Constants.CLIENT_RECV)
      case tracing.Annotation.ClientRecvError(error: String) =>
        annotate(record, ErrorAnnotation.format(thrift.Constants.CLIENT_RECV_ERROR, error))
      case tracing.Annotation.ServerSend =>
        annotate(record, thrift.Constants.SERVER_SEND)
      case tracing.Annotation.ServerRecv =>
        annotate(record, thrift.Constants.SERVER_RECV)
      case tracing.Annotation.ServerSendError(error: String) =>
        annotate(record, ErrorAnnotation.format(thrift.Constants.SERVER_SEND_ERROR, error))
      case tracing.Annotation.ClientSendFragment =>
        annotate(record, thrift.Constants.CLIENT_SEND_FRAGMENT)
      case tracing.Annotation.ClientRecvFragment =>
        annotate(record, thrift.Constants.CLIENT_RECV_FRAGMENT)
      case tracing.Annotation.ServerSendFragment =>
        annotate(record, thrift.Constants.SERVER_SEND_FRAGMENT)
      case tracing.Annotation.ServerRecvFragment =>
        annotate(record, thrift.Constants.SERVER_RECV_FRAGMENT)
      case tracing.Annotation.Message(value) =>
        annotate(record, value)
      case tracing.Annotation.Rpc(name: String) =>
        spanMap.update(record.traceId)(_.setName(name))
      case tracing.Annotation.ServiceName(serviceName: String) =>
        spanMap.update(record.traceId)(_.setServiceName(serviceName))
      case tracing.Annotation.BinaryAnnotation(key: String, value: Boolean) =>
        binaryAnnotation(
          record,
          key,
          (if (value) TrueBB else FalseBB).duplicate(),
          thrift.AnnotationType.BOOL
        )
      case tracing.Annotation.BinaryAnnotation(key: String, value: Array[Byte]) =>
        binaryAnnotation(record, key, ByteBuffer.wrap(value), thrift.AnnotationType.BYTES)
      case tracing.Annotation.BinaryAnnotation(key: String, value: ByteBuffer) =>
        binaryAnnotation(record, key, value, thrift.AnnotationType.BYTES)
      case tracing.Annotation.BinaryAnnotation(key: String, value: Short) =>
        binaryAnnotation(
          record,
          key,
          ByteBuffer.allocate(2).putShort(0, value),
          thrift.AnnotationType.I16
        )
      case tracing.Annotation.BinaryAnnotation(key: String, value: Int) =>
        binaryAnnotation(
          record,
          key,
          ByteBuffer.allocate(4).putInt(0, value),
          thrift.AnnotationType.I32
        )
      case tracing.Annotation.BinaryAnnotation(key: String, value: Long) =>
        binaryAnnotation(
          record,
          key,
          ByteBuffer.allocate(8).putLong(0, value),
          thrift.AnnotationType.I64
        )
      case tracing.Annotation.BinaryAnnotation(key: String, value: Double) =>
        binaryAnnotation(
          record,
          key,
          ByteBuffer.allocate(8).putDouble(0, value),
          thrift.AnnotationType.DOUBLE
        )
      case tracing.Annotation.BinaryAnnotation(key: String, value: String) =>
        binaryAnnotation(record, key, ByteBuffer.wrap(value.getBytes), thrift.AnnotationType.STRING)
      case tracing.Annotation.BinaryAnnotation(key @ _, value @ _) => // Throw error?
      case tracing.Annotation.LocalAddr(ia: InetSocketAddress) =>
        setEndpoint(record, ia)
      case tracing.Annotation.ClientAddr(ia: InetSocketAddress) =>
        // use a binary annotation over a regular annotation to avoid a misleading timestamp
        spanMap.update(record.traceId) {
          _.addBinaryAnnotation(
            BinaryAnnotation(
              thrift.Constants.CLIENT_ADDR,
              TrueBB.duplicate(),
              thrift.AnnotationType.BOOL,
              Endpoint.fromSocketAddress(ia)
            )
          )
        }
      case tracing.Annotation.ServerAddr(ia: InetSocketAddress) =>
        spanMap.update(record.traceId) {
          _.addBinaryAnnotation(
            BinaryAnnotation(
              thrift.Constants.SERVER_ADDR,
              TrueBB.duplicate(),
              thrift.AnnotationType.BOOL,
              Endpoint.fromSocketAddress(ia)
            )
          )
        }
    }
  }

  /**
   * Sets the endpoint in the span for any future annotations. Also
   * sets the endpoint in any previous annotations that lack one.
   */
  protected def setEndpoint(record: Record, ia: InetSocketAddress): Unit = {
    spanMap.update(record.traceId)(_.setEndpoint(Endpoint.fromSocketAddress(ia).boundEndpoint))
  }

  protected def binaryAnnotation(
    record: Record,
    key: String,
    value: ByteBuffer,
    annotationType: thrift.AnnotationType
  ): Unit = {
    spanMap.update(record.traceId) { span =>
      span.addBinaryAnnotation(BinaryAnnotation(key, value, annotationType, span.endpoint))
    }
  }

  /**
   * Add this record as a time based annotation.
   */
  protected def annotate(record: Record, value: String): Unit = {
    spanMap.update(record.traceId) { span =>
      span.addAnnotation(ZipkinAnnotation(record.timestamp, value, span.endpoint))
    }
  }

  def sendSpans(spans: Seq[Span]): Future[Unit]
}
