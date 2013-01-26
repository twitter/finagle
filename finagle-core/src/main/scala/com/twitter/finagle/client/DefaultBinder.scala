package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * Binds a concrete endpoint address to a ServiceFactory that is used
 * to establish connections and dispatch requests to that endpoint.
 *
 * @param connect Connect to the endpoint named by the given
 * SocketAddress.
 *
 * @param newDispatcher Create a new dispatcher responsible for
 * coordinating requests sent to the returned service onto the given
 * transport.
 */
case class DefaultBinder[Req, Rep, In, Out](
  connect: (SocketAddress, StatsReceiver) => Future[Transport[In, Out]],
  newDispatcher: Transport[In, Out] => Service[Req, Rep]
) extends ((SocketAddress, StatsReceiver) => ServiceFactory[Req, Rep]) {
  def apply(sa: SocketAddress, statsReceiver: StatsReceiver) = 
    ServiceFactory(() => connect(sa, statsReceiver) map newDispatcher)
}
