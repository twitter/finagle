package com.twitter.finagle.thrift

import collection.JavaConversions._
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.jboss.netty.buffer.ChannelBuffers

import com.twitter.util.Time
import com.twitter.finagle.tracing.{
  Record, Annotation, FrozenTranscript,
  Transcript, Span, Endpoint}

private[thrift] object ThriftTracing {
  /**
   * v1: transaction id frame
   * v2: full tracing header
   * v3: b3 (big-brother-bird)
   */
  val CanTraceMethodName = "__can__finagle__trace__v3__"
}

private[thrift] object RichSpan {
  val Utf8 = Charset.forName("UTF-8")
}

private[thrift] class RichThriftSpan(self: thrift.Span) {
  import RichSpan._

  private[this] def endpointFromThrift(endpoint: thrift.Endpoint): Option[Endpoint] =
    Option(endpoint) map { e => Endpoint(e.getIpv4, e.getPort) }

  /**
   * Creates a Finagle span from this thrift span.
   */
  def toFinagleSpan: Span = Span(
    self.getId,
    if (self.isSetParent_id) Some(self.getParent_id) else None,
    if (self.isSetTrace_id)  Some(self.getTrace_id)  else None,
    toTranscript,
    endpointFromThrift(self.getClient),
    endpointFromThrift(self.getServer),
    Seq()
  )

  /**
   * Translate this thrift-encoded span into a transcript.
   */
  def toTranscript: Transcript = {
    val records = self.annotations map { annotation =>
      val value = annotation.value match {
        case thrift.Constants.CLIENT_SEND => Annotation.ClientSend()
        case thrift.Constants.CLIENT_RECV => Annotation.ClientRecv()
        case thrift.Constants.SERVER_SEND => Annotation.ServerSend()
        case thrift.Constants.SERVER_RECV => Annotation.ServerRecv()
        case value                        => Annotation.Message(value)
      }

      Record(Time.fromMilliseconds(annotation.timestamp), value)
    }

    new FrozenTranscript(records)
  }
}

private[thrift] class RichSpan(self: Span) {
  private[this] def endpointFromFinagle(endpoint: Endpoint): thrift.Endpoint = {
    val e = new thrift.Endpoint
    e.setIpv4(endpoint.ipv4)
    e.setPort(endpoint.port)
    e
  }

  /**
   * Translate this transcript to a set of spans. A transcript may
   * contain annotations from several spans.
   */
  def toThriftSpans: Seq[thrift.Span] = {
    val span = new thrift.Span

    span.setId(self.id)
    self.parentId foreach { span.setParent_id(_) }
    span.setTrace_id(self.rootId)

    // Endpoints (optional)
    self.client foreach { e => span.setClient(endpointFromFinagle(e)) }
    self.server foreach { e => span.setServer(endpointFromFinagle(e)) }

    val annotations = self.transcript map { record =>
      val value = record.annotation match {
        case Annotation.ClientSend()   => thrift.Constants.CLIENT_SEND
        case Annotation.ClientRecv()   => thrift.Constants.CLIENT_RECV
        case Annotation.ServerSend()   => thrift.Constants.SERVER_SEND
        case Annotation.ServerRecv()   => thrift.Constants.SERVER_RECV
        case Annotation.Message(value) => value
      }

      val annotation = new thrift.Annotation
      annotation.setTimestamp(record.timestamp.inMilliseconds.toLong)
      annotation.setValue(value)
      annotation
    }

    annotations foreach { span.addToAnnotations(_) }

    val childThriftSpans =
      self.children map { new RichSpan(_) } flatMap { _.toThriftSpans }
    Seq(span) ++ childThriftSpans
  }
}

private[thrift] object conversions {
  implicit def thriftSpanToRichThriftSpan(span: thrift.Span) =
    new RichThriftSpan(span)
  implicit def spanToRichSpan(span: Span) =
    new RichSpan(span)
}

