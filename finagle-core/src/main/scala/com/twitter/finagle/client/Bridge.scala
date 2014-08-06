package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import java.net.SocketAddress

object Bridge {
  /**
   * Bridges a transporter with a dispatcher, returning a function that,
   * when invoked, yields a ServiceFactory representing a concrete
   * endpoint. In reality this is just as small utility to wire up dispatchers
   * with transporters, but its explicit name provides clarity and purpose.
   *
   * It is a bridge in the sense that it reconciles the stream-oriented
   * Transport, with the request-response-oriented ServiceFactory.
   *
   * @param newDispatcher Create a new dispatcher responsible for
   * coordinating requests sent to the returned service onto the given
   * transport.
   */
  def apply[In, Out, Req, Rep](
    transporter: (SocketAddress, StatsReceiver) => Future[Transport[In, Out]],
    newDispatcher: Transport[In, Out] => Service[Req, Rep]
  ): ((SocketAddress, StatsReceiver) => ServiceFactory[Req, Rep]) =
    (sa, sr) => ServiceFactory(() => transporter(sa, sr) map newDispatcher)
}
