package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.{http, _}
import com.twitter.util.Future
import io.opencensus.trace.Tracing
import io.opencensus.trace.propagation.TextFormat

/**
 * Syntax enhancements to Finagle clients to add OpenCensus tracing
 * headers to requests.
 *
 * HTTP and ThriftMux protocols are supported.
 *
 * Servers should also participate by using [[StackServerOps]].
 *
 * @see [[StackServerOps]]
 *
 * @example
 * Scala:
 * {{{
 * import com.twitter.finagle.Http
 * import com.twitter.finagle.tracing.opencensus.StackClientOps._
 *
 * val clientWithOpenCensusTracing = Http.client.withOpenCensusTracing
 * }}}
 *
 * Java users can explicitly use a [[StackClientOps]] class:
 * {{{
 * import com.twitter.finagle.Http;
 * import com.twitter.finagle.tracing.opencensus.StackClientOps.HttpOpenCensusTracing;
 *
 * Http.Client clientWithOpenCensusTracing =
 *   new HttpOpenCensusTracing(Http.client()).withOpenCensusTracing();
 * }}}
 */
object StackClientOps {

  implicit final class ThriftMuxOpenCensusTracing(private val client: ThriftMux.Client)
      extends AnyVal {
    def withOpenCensusTracing: ThriftMux.Client =
      client.withStack(_.prepend(ClientTraceContextFilter.module))
  }

  implicit final class HttpOpenCensusTracing(private val client: Http.Client) extends AnyVal {
    def withOpenCensusTracing: Http.Client =
      withOpenCensusTracing(Tracing.getPropagationComponent.getB3Format)

    def withOpenCensusTracing(textFormat: TextFormat): Http.Client =
      client.withStack { stack =>
        // serialization must happen after we attach the OpenCensus span
        stack
          .insertBefore(
            TraceInitializerFilter.role,
            ClientTraceContextFilter.module[http.Request, http.Response]
          )
          .replace(TraceInitializerFilter.role, httpSerializeModule(textFormat))
      }
  }

  private[this] def httpSerializeFilter(
    textFormat: TextFormat
  ): SimpleFilter[http.Request, http.Response] =
    new SimpleFilter[http.Request, http.Response] {
      private[this] val setter = new TextFormat.Setter[http.Request] {
        def put(carrier: http.Request, key: String, value: String): Unit =
          carrier.headerMap.set(key, value)
      }

      def apply(
        request: http.Request,
        service: Service[http.Request, http.Response]
      ): Future[http.Response] = {
        Contexts.broadcast.get(TraceContextFilter.SpanContextKey) match {
          case Some(spanCtx) => textFormat.inject(spanCtx, request, setter)
          case None => // no headers to propagate
        }
        service(request)
      }
    }

  /** exposed for testing */
  private[opencensus] val HttpSerializationStackRole: Stack.Role =
    Stack.Role("OpenCensusHeaderSerialization")

  private[this] def httpSerializeModule(
    textFormat: TextFormat
  ): Stackable[ServiceFactory[http.Request, http.Response]] =
    new Stack.Module0[ServiceFactory[http.Request, http.Response]] {
      def make(
        next: ServiceFactory[http.Request, http.Response]
      ): ServiceFactory[http.Request, http.Response] =
        httpSerializeFilter(textFormat).andThen(next)

      def role: Stack.Role = HttpSerializationStackRole

      def description: String = "Adds OpenCensus HTTP Headers"
    }

}
