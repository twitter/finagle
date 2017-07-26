package com.twitter.finagle.tracing

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import java.net.InetSocketAddress

/**
 * [[com.twitter.finagle.ServiceFactoryProxy]] used to trace the local addr and
 * server addr.
 */
class ServerDestTracingProxy[Req, Rep](self: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy[Req, Rep](self) {
  override def apply(conn: ClientConnection) = {
    // this filter gymnastics is done so that annotation occurs after
    // traceId is set by any inbound request with tracing enabled
    val filter = new SimpleFilter[Req, Rep] {
      def apply(request: Req, service: Service[Req, Rep]) = {
        if (Trace.isActivelyTracing) {
          conn.localAddress match {
            case ia: InetSocketAddress =>
              Trace.recordLocalAddr(ia)
              Trace.recordServerAddr(ia)
            case _ => // do nothing for non-ip address
          }
          conn.remoteAddress match {
            case ia: InetSocketAddress =>
              Trace.recordClientAddr(ia)
            case _ => // do nothing for non-ip address
          }
        }
        service(request)
      }
    }

    self(conn) map { filter andThen _ }
  }
}

private[finagle] object ClientDestTracingFilter {
  val role = Stack.Role("EndpointTracing")

  /**
   * $module [[com.twitter.finagle.tracing.ClientDestTracingFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[Transporter.EndpointAddr, param.Tracer, ServiceFactory[Req, Rep]] {
      val role = ClientDestTracingFilter.role
      val description = "Record remote address of server"
      def make(
        _addr: Transporter.EndpointAddr,
        _tracer: param.Tracer,
        next: ServiceFactory[Req, Rep]
      ) = {
        val param.Tracer(tracer) = _tracer
        if (tracer.isNull) next
        else {
          val Transporter.EndpointAddr(addr) = _addr
          new ClientDestTracingFilter(addr) andThen next
        }
      }
    }
}

/**
 * [[com.twitter.finagle.Filter]] for clients to record the remote address of the server.
 * We don't log the local addr here because it's already done in the client Dispatcher.
 */
class ClientDestTracingFilter[Req, Rep](addr: Address) extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]) = {
    val ret = service(request)
    addr match {
      case Address.Inet(ia, _) =>
        Trace.recordServerAddr(ia)
      case _ => // do nothing for non-ip address
    }
    ret
  }
}
