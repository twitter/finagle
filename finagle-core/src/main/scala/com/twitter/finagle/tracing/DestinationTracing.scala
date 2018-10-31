package com.twitter.finagle.tracing

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.util.Future
import java.net.InetSocketAddress

/**
 * [[com.twitter.finagle.ServiceFactoryProxy]] used to trace the local addr and
 * server addr.
 */
private[finagle] class ServerDestTracingProxy[Req, Rep](self: ServiceFactory[Req, Rep])
    extends ServiceFactoryProxy[Req, Rep](self) {

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    self(conn).map(sf => new ServerDestTracingFilter[Req, Rep](conn).andThen(sf))
}

private final class ServerDestTracingFilter[Req, Rep](
  conn: ClientConnection
) extends SimpleFilter[Req, Rep] {

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      conn.localAddress match {
        case ia: InetSocketAddress =>
          trace.recordLocalAddr(ia)
          trace.recordServerAddr(ia)
        case _ => // do nothing for non-ip address
      }
      conn.remoteAddress match {
        case ia: InetSocketAddress =>
          trace.recordClientAddr(ia)
        case _ => // do nothing for non-ip address
      }
    }

    service(request)
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
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val trace = Trace()
    val rep = service(request)

    if (trace.isActivelyTracing) {
      addr match {
        case Address.Inet(ia, _) =>
          trace.recordServerAddr(ia)
        case _ => // do nothing for non-ip address
      }
    }

    rep
  }
}
