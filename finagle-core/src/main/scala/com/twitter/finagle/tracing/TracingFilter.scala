package com.twitter.finagle.tracing

import com.twitter.finagle._

private[finagle] object TracingFilter {
  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.tracing.TracingFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]](param.Tracer) {
      def make(params: Params, next: ServiceFactory[Req, Rep]) = {
        val param.Tracer(tracer) = params[param.Tracer]
        new TracingFilter(tracer) andThen next
      }
    }
}

/**
 * The TracingFilter takes care of span lifecycle events. It is always
 * placed first in the server filter chain so that protocols with
 * trace support will override the span resets, and still be properly
 * reported here.
 */
class TracingFilter[Req, Rep](tracer: Tracer) extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]) = {
    Trace.unwind {
      Trace.pushTracerAndSetNextId(tracer)
      Trace.recordBinary("finagle.version", Init.finagleVersion)
      service(request)
    }
  }
}
