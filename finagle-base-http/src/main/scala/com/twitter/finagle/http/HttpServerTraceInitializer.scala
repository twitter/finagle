package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.tracing.{Trace, TraceInitializerFilter}
import com.twitter.finagle.{Filter, ServiceFactory, Stack}

private[finagle] class HttpServerTraceInitializer[Req <: Request, Rep]
    extends Stack.Module1[finagle.param.Tracer, ServiceFactory[Req, Rep]] {
  val role: Stack.Role = TraceInitializerFilter.role
  val description: String =
    "Initialize the tracing system with trace info from the incoming request"

  def make(
    _tracer: finagle.param.Tracer,
    next: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] = {
    val finagle.param.Tracer(tracer) = _tracer
    val traceInitializer = Filter.mk[Req, Rep, Req, Rep] { (req, svc) =>
      Trace.letTracer(tracer) {
        TraceInfo.letTraceIdFromRequestHeaders(req) { svc(req) }
      }
    }
    traceInitializer.andThen(next)
  }
}
