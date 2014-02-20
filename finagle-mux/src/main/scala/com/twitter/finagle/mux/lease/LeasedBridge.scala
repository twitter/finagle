package com.twitter.finagle.mux.lease

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import java.net.SocketAddress

private[finagle] trait Acting {
  def isActive: Boolean
}

private[finagle] object LeasedBridge {

  /**
   * Bridges a transporter with a leased dispatcher, so that the resulting
   * function is a [[com.twitter.finagle.ServiceFactory]] that has an
   * isAvailable that reflects whether the outstanding
   * [[com.twitter.finagle.Service Services]] created by the factory have the
   * lease or not.
   */
  def apply[In, Out, Req, Rep](
    transporter: (SocketAddress, StatsReceiver) => Future[Transport[In, Out]],
    newDispatcher: Transport[In, Out] => Service[Req, Rep] with Acting
  ): ((SocketAddress, StatsReceiver) => ServiceFactory[Req, Rep]) = { (sa, sr) =>
    new LeasedFactory(() => (transporter(sa, sr) map newDispatcher))
  }
}
