package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.context.Contexts
import com.twitter.finagle._
import com.twitter.util.{Future, Try}
import io.opencensus.trace.{SpanContext, Tracing}
import io.opencensus.trace.propagation.TextFormat

/**
 * Syntax enhancements to Finagle servers to attach OpenCensus tracing
 * headers from requests.
 *
 * HTTP and ThriftMux protocols are supported.
 *
 * Clients should also participate by using [[StackClientOps]].
 *
 * @see [[StackClientOps]]
 *
 * @example
 * Scala:
 * {{{
 * import com.twitter.finagle.Http
 * import com.twitter.finagle.tracing.opencensus.StackServerOps._
 *
 * val serverWithOpenCensusTracing = Http.server.withOpenCensusTracing
 * }}}
 *
 * Java users can explicitly use a [[StackServerOps]] class:
 * {{{
 * import com.twitter.finagle.Http;
 * import com.twitter.finagle.tracing.opencensus.StackServerOps.HttpOpenCensusTracing;
 *
 * Http.Server serverWithOpenCensusTracing =
 *   new HttpOpenCensusTracing(Http.server()).withOpenCensusTracing();
 * }}}
 */
object StackServerOps {

  implicit final class ThriftMuxOpenCensusTracing(private val server: ThriftMux.Server)
      extends AnyVal {
    def withOpenCensusTracing: ThriftMux.Server = {
      server.withStack(_.prepend(ServerTraceContextFilter.module))
    }
  }

  implicit final class HttpOpenCensusTracing(private val server: Http.Server) extends AnyVal {
    def withOpenCensusTracing: Http.Server =
      withOpenCensusTracing(Tracing.getPropagationComponent.getB3Format)

    def withOpenCensusTracing(textFormat: TextFormat): Http.Server = {
      server.withStack { stack =>
        stack
          .prepend(ServerTraceContextFilter.module)
          .prepend(httpDeserModule(textFormat)) // attach to broadcast ctx before setting OC
      }
    }
  }

  private def httpDeserFilter(textFormat: TextFormat): SimpleFilter[http.Request, http.Response] =
    new SimpleFilter[http.Request, http.Response] {
      private[this] val getter = new TextFormat.Getter[http.Request] {
        def get(carrier: http.Request, key: String): String =
          carrier.headerMap.getOrNull(key)
      }

      def apply(
        request: http.Request,
        service: Service[http.Request, http.Response]
      ): Future[http.Response] = {
        val spanContext = Try(textFormat.extract(request, getter)).getOrElse(SpanContext.INVALID)
        if (spanContext != SpanContext.INVALID) {
          Contexts.broadcast.let(TraceContextFilter.SpanContextKey, spanContext) {
            service(request)
          }
        } else {
          service(request)
        }
      }
    }

  /** exposed for testing */
  private[opencensus] val HttpDeserializationStackRole: Stack.Role =
    Stack.Role("OpenCensusHeaderDeserialization")

  private def httpDeserModule(
    textFormat: TextFormat
  ): Stackable[ServiceFactory[http.Request, http.Response]] =
    new Stack.Module0[ServiceFactory[http.Request, http.Response]] {
      def make(
        next: ServiceFactory[http.Request, http.Response]
      ): ServiceFactory[http.Request, http.Response] =
        httpDeserFilter(textFormat).andThen(next)

      def role: Stack.Role = HttpDeserializationStackRole

      def description: String = "Attaches OpenCensus HTTP Headers to the broadcast context"
    }

}
