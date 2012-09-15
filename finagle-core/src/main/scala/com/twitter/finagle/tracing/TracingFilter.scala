package com.twitter.finagle.tracing

/**
 * The TracingFilter takes care of span lifecycle events. It is always
 * placed first in the server filter chain so that protocols with
 * trace support will override the span resets, and still be properly
 * reported here.
 */

import com.twitter.finagle.{Service, SimpleFilter}

class TracingFilter[Req, Rep](tracer: Tracer)
  extends SimpleFilter[Req, Rep]
{
  def apply(request: Req, service: Service[Req, Rep]) = {
    Trace.unwind {
      Trace.pushTracer(tracer)
      val nextId = Trace.nextId
      Trace.setId(nextId.copy(_sampled =
        if (Trace.id.sampled.isDefined) Trace.id.sampled else tracer.sampleTrace(nextId)))
      service(request)
    }
  }
}
