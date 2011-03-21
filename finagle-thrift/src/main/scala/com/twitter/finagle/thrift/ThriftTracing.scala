package com.twitter.finagle.thrift

import collection.JavaConversions._

import com.twitter.util.Time
import com.twitter.finagle.tracing.{SpanId, Record, Annotation, FrozenTranscript, Transcript}

object ThriftTracing {
  /**
   * v1: transaction id frame
   * v2: full tracing header
   * v3: b3 (big-brother-bird)
   */
  val CanTraceMethodName = "__can__finagle__trace__v3__"
}

class RichSpan(self: thrift.Span) {
  /**
   * Translate this thrift-encoded span into a transcript.
   */
  def toTranscript: Transcript = {
    val spanId = SpanId(
      id       = self.getId,
      parentId = if (self.isSetParent_id)   Some(self.getParent_id) else None,
      _rootId  = if (self.isSetTrace_id)    Some(self.getTrace_id)  else None,
      host     = if (self.isSetClient_host) self.getClient_host     else 0,
      vm       = ""  // TODO? will this be an annotation?
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

class RichTranscript(self: Transcript) {
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

object conversions {
  implicit def spanToRichSpan(span: thrift.Span) =
    new RichSpan(span)
  implicit def transcriptToRichTranscript(transcript: Transcript) =
    new RichTranscript(transcript)
}
