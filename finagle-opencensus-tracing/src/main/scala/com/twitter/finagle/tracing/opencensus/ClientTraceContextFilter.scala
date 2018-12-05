package com.twitter.finagle.tracing.opencensus

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack, Stackable, param}
import com.twitter.finagle.tracing.opencensus.TracingOps._
import com.twitter.util.Future
import io.opencensus.trace.Tracing

object ClientTraceContextFilter {
  val role: Stack.Role = Stack.Role("OpenCensusClientTraceContextModule")

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Label, ServiceFactory[Req, Rep]] {
      def make(label: param.Label, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        new ClientTraceContextFilter(label.label).andThen(next)

      def role: Stack.Role = ClientTraceContextFilter.role

      def description: String =
        "Sends the current OpenCensus trace context across RPC calls and adds a span for the call."
    }
}

/**
 * Adds an OpenCensus trace context that will be sent to the backend server
 * using Finagle's broadcast contexts. The backend service should
 * install a [[ServerTraceContextFilter]] to attach on to this.
 *
 * A `Span` is created per request and is ended when the request's `Future` is
 * satisfied.
 *
 * @see [[StackClientOps]] for client installation instructions.
 */
final class ClientTraceContextFilter[Req, Resp](_label: String) extends SimpleFilter[Req, Resp] {
  private[this] final val label = s"/rpc/${_label}"

  def apply(request: Req, service: Service[Req, Resp]): Future[Resp] = {
    val currentContext = Tracing.getTracer.getCurrentSpan
    if (currentContext.getContext.isValid) {
      val rpcSpan = Tracing.getTracer
        .spanBuilder(label)
        .startSpan()

      rpcSpan.scopedToFutureAndEnd {
        Contexts.broadcast.let(TraceContextFilter.SpanContextKey, rpcSpan.getContext) {
          service(request)
        }
      }
    } else {
      service(request)
    }
  }
}
