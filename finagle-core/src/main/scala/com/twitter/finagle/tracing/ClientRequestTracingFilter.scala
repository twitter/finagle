package com.twitter.finagle.tracing

import com.twitter.finagle.{Service, SimpleFilter}

/**
 * Adds the basic tracing information to a request.
 * Includes: rpc service name, method name, client sent and client received.
 */
trait ClientRequestTracingFilter[Req, Res] extends SimpleFilter[Req, Res] {
  def apply(
    request: Req,
    service: Service[Req, Res]
  ) = {
    Trace.recordRpcname(serviceName, methodName(request))
    Trace.record(Annotation.ClientSend())

    service(request) onSuccess { _ =>
      Trace.record(Annotation.ClientRecv())
    }
  }

  val serviceName: String
  def methodName(req: Req): String
}
