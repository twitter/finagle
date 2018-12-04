package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.context.Contexts
import com.twitter.io.Buf
import com.twitter.util.{Return, Throw, Try}
import io.opencensus.trace.{SpanContext, Tracing}
import scala.util.control.NonFatal

private[opencensus] object TraceContextFilter {
  val SpanContextKey: Contexts.broadcast.Key[SpanContext] =
    new Contexts.broadcast.Key[SpanContext]("io.opencensus.trace.SpanContext") {
      private[this] def marshaller = Tracing.getPropagationComponent.getBinaryFormat

      def marshal(value: SpanContext): Buf =
        Buf.ByteArray.Owned(marshaller.toByteArray(value))

      def tryUnmarshal(buf: Buf): Try[SpanContext] =
        try {
          val bytes = Buf.ByteArray.Owned.extract(buf)
          Return(marshaller.fromByteArray(bytes))
        } catch {
          case NonFatal(e) => Throw(e)
        }
    }
}
