package com.twitter.finagle.tracing

import com.twitter.finagle.{Init, Service, SimpleFilter}

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
