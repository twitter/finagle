package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.tracing.{Trace, TraceInitializerFilter, Tracer}
import com.twitter.finagle.{Filter, ServiceFactory, Stack}

private[finagle] object HttpClientTraceInitializer {

  def apply[Req, Rep](tracer: Tracer): Filter[Req, Rep, Req, Rep] =
    Filter.mk[Req, Rep, Req, Rep] { (req, svc) =>
      Trace.letTracerAndNextId(tracer) {
        TraceInfo.setClientRequestHeaders(req.asInstanceOf[Request])
        svc(req)
      }
    }

  def typeAgnostic(tracer: Tracer): Filter.TypeAgnostic = new Filter.TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = apply(tracer)
  }

}

private[finagle] class HttpClientTraceInitializer[Req <: Request, Rep]
    extends Stack.Module1[finagle.param.Tracer, ServiceFactory[Req, Rep]] {
  val role: Stack.Role = TraceInitializerFilter.role
  val description: String =
    "Sets the next TraceId and attaches trace information to the outgoing request"
  def make(
    _tracer: finagle.param.Tracer,
    next: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] = {
    val traceInitializer = HttpClientTraceInitializer[Req, Rep](_tracer.tracer)
    traceInitializer.andThen(next)
  }
}
