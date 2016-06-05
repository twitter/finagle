package com.twitter.finagle.service

import java.net.SocketAddress

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.context.RemoteInfo.Upstream
import com.twitter.finagle.thrift.ClientId
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future

private[finagle] object ExceptionRemoteInfoFactory {
  val role = Stack.Role("ExceptionRemoteInfo")

  def addRemoteInfo[T](endpointAddr: SocketAddress, label: String): PartialFunction[Throwable, Future[T]] = {
    case e: HasRemoteInfo =>
      e.setRemoteInfo(RemoteInfo.Available(
        Upstream.addr, ClientId.current, Some(endpointAddr), Some(ClientId(label)), Trace.id))
      Future.exception(e)
    case f: Failure =>
      Future.exception(f.withSource(Failure.Source.RemoteInfo, RemoteInfo.Available(
        Upstream.addr, ClientId.current, Some(endpointAddr), Some(ClientId(label)), Trace.id)))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[service.ExceptionRemoteInfoFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      val role = ExceptionRemoteInfoFactory.role
      val description = "Add upstream/downstream addresses and trace id to request exceptions"

      val parameters = Seq(implicitly[Stack.Param[Transporter.EndpointAddr]])

      def make(params: Stack.Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val endpointAddr = params[Transporter.EndpointAddr].addr
        endpointAddr match {
          case Address.Inet(addr, _) =>
            val param.Label(label) = params[param.Label]
            new ExceptionRemoteInfoFactory(next, addr, label)
          case _ => next
        }


      }
    }
}

/**
 * A [[com.twitter.finagle.ServiceFactoryProxy]] that adds remote info to exceptions for clients.
 * The `upstreamAddr`, `downstreamAddr`, and `traceid` fields of `remoteInfo`
 * in any [[com.twitter.finagle.HasRemoteInfo]] exception thrown by the
 * underlying [[com.twitter.finagle.Service]] are set to the `upstreamAddr`
 * read from the local context, the `downstreamAddr` argument to this factory,
 * and the `traceid` read from the local context, respectively.
 */
private[finagle] class ExceptionRemoteInfoFactory[Req, Rep](
    underlying: ServiceFactory[Req, Rep],
    endpointAddr: SocketAddress,
    label: String)
  extends ServiceFactoryProxy[Req, Rep](underlying)
{
  private[this] val requestAddRemoteInfo: PartialFunction[Throwable, Future[Rep]] =
    ExceptionRemoteInfoFactory.addRemoteInfo(endpointAddr, label)

  private[this] val connectionAddRemoteInfo: PartialFunction[Throwable, Future[Service[Req,Rep]]] =
    ExceptionRemoteInfoFactory.addRemoteInfo(endpointAddr, label)

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    underlying(conn).map { service =>
      val filter = new SimpleFilter[Req, Rep] {
        override def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
          service(request).rescue(requestAddRemoteInfo)
      }
      filter andThen service
    }.rescue(connectionAddRemoteInfo)

}
