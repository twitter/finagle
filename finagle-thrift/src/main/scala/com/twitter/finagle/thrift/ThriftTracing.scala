package com.twitter.finagle.thrift

import collection.JavaConversions._
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.jboss.netty.buffer.ChannelBuffers

import com.twitter.util.Time
import com.twitter.finagle.tracing.{SpanId, Record, Annotation, FrozenTranscript, Transcript}

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

private[thrift] class RichSpan(self: thrift.Span) {
  import RichSpan._

  /**
   * Translate this thrift-encoded span into a transcript.
   */
  def toTranscript: Transcript = {
    val vmid =
      Option(self.binary_annotations) flatMap { annotations =>
        Option(annotations.get(thrift.Constants.VM_KEY))
      } map { ChannelBuffers.copiedBuffer(_).toString(Utf8) }

    val spanId = SpanId(
      id       = self.getId,
      parentId = if (self.isSetParent_id)   Some(self.getParent_id) else None,
      _rootId  = if (self.isSetTrace_id)    Some(self.getTrace_id)  else None,
      host     = if (self.isSetClient_host) self.getClient_host     else 0,
      vm       = vmid getOrElse "unknown"
    )

    val records = self.annotations map { annotation =>
      val value = annotation.value match {
        case thrift.Constants.CLIENT_SEND => Annotation.ClientSend()
        case thrift.Constants.CLIENT_RECV => Annotation.ClientRecv()
        case thrift.Constants.SERVER_SEND => Annotation.ServerSend()
        case thrift.Constants.SERVER_RECV => Annotation.ServerRecv()
        case value                        => Annotation.Message(value)
      }

      Record(spanId, Time.fromMilliseconds(annotation.timestamp), value)
    }

    new FrozenTranscript(records)
  }
}

private[thrift] class RichTranscript(self: Transcript) {
  /**
   * Translate this transcript to a set of spans. A transcript may
   * contain annotations from several spans.
   */
  def toThriftSpans: Seq[thrift.Span] = {
    (self groupBy { _.spanId } toSeq) map { case (spanId, records) =>
      val span = new thrift.Span
      span.setId(spanId.id)
      span.setServer_host(spanId.host)
      spanId._rootId  foreach { span.setTrace_id(_) }
      spanId.parentId foreach { span.setParent_id(_) }

      span.putToBinary_annotations(
        thrift.Constants.VM_KEY,
        ByteBuffer.wrap(spanId.vm.getBytes("UTF-8")))

      val annotations = records map { record =>
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
      span
    }
  }
}

private[thrift] object conversions {
  implicit def spanToRichSpan(span: thrift.Span) =
    new RichSpan(span)
  implicit def transcriptToRichTranscript(transcript: Transcript) =
    new RichTranscript(transcript)
}
