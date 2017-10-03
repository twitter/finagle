package com.twitter.finagle.tracing

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Future, Return, Try}

/**
 * Adds the basic tracing information to a request.
 * Includes: rpc service name, method name, client sent and client received.
 */
trait ClientRequestTracingFilter[Req, Res] extends SimpleFilter[Req, Res] {

  private[this] val recordClientReceive: Try[Res] => Unit = {
    case Return(_) =>
      Trace.record(Annotation.ClientRecv())
    case _ =>
  }

  def apply(
    request: Req,
    service: Service[Req, Res]
  ): Future[Res] = {
    if (Trace.isActivelyTracing) {
      Trace.recordServiceName(serviceName)
      Trace.recordRpc(methodName(request))
      Trace.record(Annotation.ClientSend())

      service(request).respond(recordClientReceive)
    } else
      service(request)
  }

  val serviceName: String
  def methodName(req: Req): String
}
