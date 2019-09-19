package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.Status
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.util.Future
import io.netty.handler.codec.http.LastHttpContent

/**
 * `Transport` proxy whos status is closed after reading the `LastHttpContent`.
 *
 * This is useful for the first client transports emitted from the Netty multiplex
 * based H2C and TLS Transporters which will send the dispatcher a `Transport`
 * backed by the H2 session which is only good for a single dispatch. We don't
 * want that generated `Service` to look healthy and get put back in the default
 * pool when in fact it is no longer of value, so we signal that we're
 * `Status.Closed` after reading the end of the HTTP stream, signaled by the Netty
 * `LastHttpContent`.
 */
private final class SingleDispatchTransport(self: Transport[Any, Any])
    extends TransportProxy[Any, Any](self) {

  @volatile
  private[this] var lastContentSeen = false

  def write(req: Any): Future[Unit] = self.write(req)

  // Although we don't transform the value we use a `.map` instead of `.respond`
  // or non-transforming continuation methods to force the sequence of events.
  def read(): Future[Any] = self.read().map { v =>
    observeMessage(v)
    v
  }

  override def status: Status =
    if (lastContentSeen) Status.Closed
    else super.status

  private[this] def observeMessage(msg: Any): Unit = msg match {
    case _: LastHttpContent => lastContentSeen = true
    case _ => // nop
  }
}
