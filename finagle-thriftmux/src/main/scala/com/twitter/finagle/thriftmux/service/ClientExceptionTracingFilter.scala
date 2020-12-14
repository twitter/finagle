package com.twitter.finagle.thriftmux.service

import com.twitter.finagle._
import com.twitter.finagle.filter.{ClientExceptionTracingFilter => ExceptionTracingFilter}
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.tracing.Tracing
import com.twitter.io.Buf
import com.twitter.util.{Future, Return, Throw}
import scala.util.control.NonFatal

/**
 * Reports error and exception annotations when a span completes.
 * Deserializes responses to get more accurate error annotations reported
 * at the right time.
 */
final class ClientExceptionTracingFilter extends ExceptionTracingFilter[mux.Request, mux.Response] {
  override def handleResponse(tracing: Tracing, rep: Future[mux.Response]): Future[mux.Response] = {
    rep.respond {
      case Throw(e) => traceError(tracing, e)
      case Return(rep: mux.Response) =>
        val deserCtx = ClientDeserializeCtx.get
        if (deserCtx ne ClientDeserializeCtx.nullDeserializeCtx) {
          try {
            val bytes = Buf.ByteArray.Owned.extract(rep.body)
            deserCtx.deserialize(bytes) match {
              case Throw(e) => traceError(tracing, e)
              case Return(_) =>
            }
          } catch {
            case NonFatal(e: Throwable) => traceError(tracing, e)
          }
        }
    }
  }
}
