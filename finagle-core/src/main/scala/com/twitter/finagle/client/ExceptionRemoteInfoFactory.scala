package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future
import java.net.SocketAddress

private[finagle] object ExceptionRemoteInfoFactory {
  val role: Stack.Role = Stack.Role("ExceptionRemoteInfo")

  private[this] case class UpstreamInfo(addr: Option[SocketAddress], id: Option[String])

  private[this] val UnknownUpstream = UpstreamInfo(None, None)
  private[this] val UnknownUpstreamFn: () => UpstreamInfo = () => UnknownUpstream

  private[this] val ctx = new Contexts.local.Key[UpstreamInfo]()

  /**
   * Set the local upstream info for clients.
   *
   * Servers and application users should instead be using
   * `Upstream.addr` and `ClientId.current`
   */
  def letUpstream[R](addr: Option[SocketAddress], id: Option[String])(f: => R): R =
    Contexts.local.let(ctx, UpstreamInfo(addr, id))(f)

  private[this] def currentUpstream: UpstreamInfo =
    Contexts.local.getOrElse(ctx, UnknownUpstreamFn)

  def addRemoteInfo[T](
    downstreamAddr: SocketAddress,
    downstreamLabel: String
  ): PartialFunction[Throwable, Future[T]] = {
    case e: HasRemoteInfo =>
      e.setRemoteInfo(
        RemoteInfo.Available(
          currentUpstream.addr,
          currentUpstream.id,
          Some(downstreamAddr),
          Some(downstreamLabel),
          Trace.id
        )
      )
      Future.exception(e)
    case f: Failure =>
      Future.exception(
        f.withSource(
          Failure.Source.RemoteInfo,
          RemoteInfo.Available(
            currentUpstream.addr,
            currentUpstream.id,
            Some(downstreamAddr),
            Some(downstreamLabel),
            Trace.id
          )
        )
      )
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[client.ExceptionRemoteInfoFactory]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      val role: Stack.Role = ExceptionRemoteInfoFactory.role
      val description: String =
        "Annotate HasRemoteInfo exceptions with upstream, downstream, and trace"

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
  downstreamAddr: SocketAddress,
  downstreamLabel: String)
    extends ServiceFactoryProxy[Req, Rep](underlying) {

  private[this] val requestAddRemoteInfo: PartialFunction[Throwable, Future[Rep]] =
    ExceptionRemoteInfoFactory.addRemoteInfo(downstreamAddr, downstreamLabel)

  private[this] val connectionAddRemoteInfo: PartialFunction[Throwable, Future[Service[Req, Rep]]] =
    ExceptionRemoteInfoFactory.addRemoteInfo(downstreamAddr, downstreamLabel)

  private[this] val filter = new SimpleFilter[Req, Rep] {
    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
      service(request).rescue(requestAddRemoteInfo)
  }

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    underlying(conn)
      .map { service => filter.andThen(service) }
      .rescue(connectionAddRemoteInfo)

}
