package com.twitter.finagle.tracing

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.util.Future
import java.net.InetSocketAddress

private[finagle] object ServerDestTracingFilter {
  val role = Stack.Role("ServerDestinationTracing")

  /**
   * Applies the [[com.twitter.finagle.tracing.ServerDestTracingFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
      val role = ServerDestTracingFilter.role
      val description = "Record incoming requests"
      def make(_tracer: param.Tracer, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        if (_tracer.tracer.isNull) next
        else {
          new ServiceFactoryProxy[Req, Rep](next) {
            override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
              next(conn).map { svc =>
                new ServerDestTracingFilter(conn).andThen(svc)
              }
          }
        }
      }
    }
}

/**
 * [[com.twitter.finagle.Filter]] used to trace the local and server addr.
 */
private final class ServerDestTracingFilter[Req, Rep](conn: ClientConnection)
    extends SimpleFilter[Req, Rep] {

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
  val ProtocolAnnotationKey = "clnt/finagle.protocol"

  /**
   * Applies the [[com.twitter.finagle.tracing.ClientDestTracingFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[
      Transporter.EndpointAddr,
      param.ProtocolLibrary,
      param.Tracer,
      ServiceFactory[Req, Rep]
    ] {
      val role = ClientDestTracingFilter.role
      val description = "Record remote address of server"
      def make(
        _addr: Transporter.EndpointAddr,
        _protocol: param.ProtocolLibrary,
        _tracer: param.Tracer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        if (_tracer.tracer.isNull) next
        else new ClientDestTracingFilter(_protocol.name, _addr.addr).andThen(next)
      }
    }
}

/**
 * [[com.twitter.finagle.Filter]] for clients to record the remote address and protocol of the server.
 * We don't log the local addr here because it's already done in the client Dispatcher.
 */
private final class ClientDestTracingFilter[Req, Rep](protocol: String, addr: Address)
    extends SimpleFilter[Req, Rep] {
  import ClientDestTracingFilter.ProtocolAnnotationKey

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      trace.recordBinary(ProtocolAnnotationKey, protocol)

      addr match {
        // this filter is placed lower in the stack where the endpoint is resolved
        case Address.Inet(sa, _) =>
          trace.recordServerAddr(sa)

        case _ => // do nothing for non-ip address
      }
    }

    service(request)
  }
}
