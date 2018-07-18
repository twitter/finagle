package com.twitter.finagle.tracing

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Future, Return}

/**
 * Adds the basic tracing information to a request.
 * Includes: rpc service name, method name, client sent and client received.
 */
trait ClientRequestTracingFilter[Req, Res] extends SimpleFilter[Req, Res] {

  def apply(request: Req, service: Service[Req, Res]): Future[Res] = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      trace.recordServiceName(serviceName)
      trace.recordRpc(methodName(request))
      trace.record(Annotation.ClientSend)
      service(request).respond {
        case Return(_) => trace.record(Annotation.ClientRecv)
        case _ =>
      }
    } else service(request)
  }

  def serviceName: String
  def methodName(req: Req): String
}
