package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Time}

/**
 * [[ClientSession]] proxy that waits for the latch before allowing the session to close.
 *
 * This is useful for the H2C and TLS clients that have to manage the possibility that
 * the connection can be either H1 or H2. In those cases, the first request will be
 * initialized using a standard `Transport` oriented dispatch model but uses a stream from
 * the underlying `ClientSession`. In that case we don't want to close the whole session
 * while that dispatch is still outstanding.
 */
private class DeferredCloseSession(underlying: ClientSession, latch: Future[Unit])
    extends ClientSession {

  def newChildTransport(): Future[Transport[Any, Any]] =
    underlying.newChildTransport()

  def status: Status = underlying.status

  def close(deadline: Time): Future[Unit] =
    latch.before(underlying.close(deadline))
}
