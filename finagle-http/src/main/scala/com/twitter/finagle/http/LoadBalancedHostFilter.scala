package com.twitter.finagle.http

import com.twitter.finagle.Address.Inet
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.{ServiceFactory, Stack, Stackable}
import com.twitter.util.FuturePool

/** Adds host header to load balanced requests.
 *
 * @note If [[Transporter.EndpointAddr]] was not created via [[java.net.InetSocketAddress.createUnresolved]],
 *       the filter will make an asynchronous network call to determine the hostname.
 * @note Since this stack module expects a [[Transporter.EndpointAddr]] to be set, it only makes sense to configure
 *       it below a module that sets the [[Transporter.EndpointAddr]] (typically via com.twitter.finagle.loadbalancer.LoadBalancerFactory).
 * @see com.twitter.finagle.loadbalancer.LoadBalancerFactory for details on how [[Transporter.EndpointAddr]] is selected
 * @example Overriding the [[TlsFilter]] host header setting with this module:
 *          {{{
 *          import com.twitter.finagle.Http
 *          import com.twitter.finagle.loadbalancer.LoadBalancerFactory
 *          Http.client.withStack(Http.client.stack.remove(TlsFilter.role)
 *            .insertAfter(LoadBalancerFactory.role, LoadBalancedHostFilter.module))
 *          }}}
 */
object LoadBalancedHostFilter {
  val role: Stack.Role = Stack.Role("LoadBalancedHost")

  def module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module1[Transporter.EndpointAddr, ServiceFactory[
      Request,
      Response
    ]] {
      val role: Stack.Role = LoadBalancedHostFilter.role
      val description =
        "Add host headers to load balanced requests"
      def make(
        addr: Transporter.EndpointAddr,
        next: ServiceFactory[Request, Response]
      ): ServiceFactory[Request, Response] =
        addr match {
          case Transporter.EndpointAddr(Inet(addr, _)) =>
            ServiceFactory(() => pool(addr.getHostName).flatMap(new TlsFilter(_).andThen(next)()))
          case _ =>
            next
        }
    }

  private[this] def pool: FuturePool = FuturePool.unboundedPool
}
