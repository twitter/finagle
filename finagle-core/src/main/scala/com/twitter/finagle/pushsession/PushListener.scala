package com.twitter.finagle.pushsession

import com.twitter.finagle.ListeningServer
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * A `PushListener` provide a method, `listen`, to expose a server on the the
 * given SocketAddress. `sessionBuilder` is called for each new connection.
 * It is furnished with a typed [[PushChannelHandle]] representing this connection
 * and expects a [[PushSession]] to be returned asynchronously, at which point
 * the session will begin to receive events. The returned [[ListeningServer]]
 * is used to inspect the server and to shut it down.
 */
trait PushListener[In, Out] {

  /**
   * Bind and listen on a socket address using the provided `sessionBuilder`
   * to build the server session.
   */
  def listen(
    addr: SocketAddress
  )(
    sessionBuilder: PushChannelHandle[In, Out] => Future[PushSession[In, Out]]
  ): ListeningServer
}
