package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack, Stackable, param}
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.tracing.opencensus.TracingOps._
import com.twitter.util.Future
import io.opencensus.trace.{AttributeValue, Tracing}

object ServerTraceContextFilter {
  private lazy val ServerInfoAttribute: AttributeValue =
    AttributeValue.stringAttributeValue(ServerInfo().id)

  val role: Stack.Role = Stack.Role("OpenCensusServerTraceContextFilter")

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Label, ServiceFactory[Req, Rep]] {
      def make(label: param.Label, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        new ServerTraceContextFilter[Req, Rep](_ => label.label).andThen(next)

      def role: Stack.Role = ServerTraceContextFilter.role

      def description: String =
        "Reads a Finagle client's OpenCensus trace context and adds a span for the call."
    }
}

/**
 *
 * Restoring an OpenCensus trace context sent by a [[ClientTraceContextFilter]]
 * into the server's OpenCensus trace context.
 *
 * A `Span` is created per request and is ended when the request's `Future` is
 * satisfied.
 *
 * @see [[StackServerOps]] for server installation instructions.
 */
final class ServerTraceContextFilter[Req, Resp](labelFn: Req => String)
    extends SimpleFilter[Req, Resp] {

  def apply(request: Req, service: Service[Req, Resp]): Future[Resp] = {
    val remoteSpanContext = Contexts.broadcast.get(TraceContextFilter.SpanContextKey)
    val spanName = s"/srv/${labelFn(request)}"
    val localSpan =
      remoteSpanContext match {
        case Some(spanContext) =>
          Tracing.getTracer
            .spanBuilderWithRemoteParent(spanName, spanContext)
            .startSpan()
        case None =>
          Tracing.getTracer
            .spanBuilderWithExplicitParent(spanName, null /* parent */ )
            .startSpan()
      }

    localSpan.putAttribute("server", ServerTraceContextFilter.ServerInfoAttribute)
    localSpan.scopedToFutureAndEnd {
      service(request)
    }
  }
}
