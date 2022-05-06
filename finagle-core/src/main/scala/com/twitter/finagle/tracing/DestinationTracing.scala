package com.twitter.finagle.tracing

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.util.Showable
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
  val NamerNameAnnotationKey = "clnt/namer.name"

  /**
   * Applies the [[com.twitter.finagle.tracing.ClientDestTracingFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module4[
      Transporter.EndpointAddr,
      BindingFactory.Dest,
      param.ProtocolLibrary,
      param.Tracer,
      ServiceFactory[Req, Rep]
    ] {
      val role = ClientDestTracingFilter.role
      val description = "Record remote address of server"
      def make(
        _addr: Transporter.EndpointAddr,
        _dest: BindingFactory.Dest,
        _protocol: param.ProtocolLibrary,
        _tracer: param.Tracer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        if (_tracer.tracer.isNull) next
        else new ClientDestTracingFilter(_protocol.name, _addr.addr, _dest.dest).andThen(next)
      }
    }
}

/**
 * [[com.twitter.finagle.Filter]] for clients to record the remote destination, address,
 * and protocol of the server. We don't log the local addr here because it's already done in the
 * client Dispatcher.
 *
 * This filter is placed lower in the stack where the endpoint is resolved so we surely have both:
 * - bound name (since we're bellow the namer/resolver)
 * - endpoint address (since we're bellow the LB)
 */
private final class ClientDestTracingFilter[Req, Rep](protocol: String, addr: Address, dest: Name)
    extends SimpleFilter[Req, Rep] {
  import ClientDestTracingFilter._

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      trace.recordBinary(ProtocolAnnotationKey, protocol)

      dest match {
        case bound: Name.Bound =>
          trace.recordBinary(NamerNameAnnotationKey, Showable.show(bound))
        case _ => // do nothing
      }

      addr match {
        case Address.Inet(sa, _) =>
          trace.recordServerAddr(sa)

        case _ => // do nothing for non-ip address
      }
    }

    service(request)
  }
}
