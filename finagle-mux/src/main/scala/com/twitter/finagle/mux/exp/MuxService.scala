package com.twitter.finagle.mux.exp

import com.twitter.concurrent.Spool
import com.twitter.finagle.{NoStacktrace, Service}
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, Return}

object UnexpectedSequencedResponseException
  extends Exception("Unexpected sequenced response")
  with NoStacktrace

/**
 * A MuxService provides the RPC, message-passing, pinging, and
 * draining facilities for one direction of a mux session.  For both
 * connectors and listeners a Session is established by providing a
 * MuxService server implementation for receiving messages. The
 * Session in turn exposes a MuxService client for sending messages.
 */
trait MuxService extends Service[Buf, Buf] {
  import Spool.*::

  /**
   * Transmits a request as a single message, and receives a single
   * message response. If more than a single message is received, an
   * exception is thrown.
   */
  final def apply(req: Buf): Future[Buf] = {
    apply(req *:: Future.value(Spool.empty[Buf])) map {
      case buf *:: Future(Return(Spool.Empty)) =>
        buf
      case buf *:: tail =>
        val exc = UnexpectedSequencedResponseException
        tail.raise(UnexpectedSequencedResponseException)
        throw UnexpectedSequencedResponseException
    }
  }

  /**
   * Transmits a request as a sequence of messages, and receives
   * response as a sequence of messages.
   */
  def apply(req: Spool[Buf]): Future[Spool[Buf]]

  /**
   * Transmit a one-way message.
   */
  def send(buf: Buf): Future[Unit]

  /**
   * Send a ping to the other party.
   */
  def ping(): Future[Unit]

  /**
   * A notification that the caller is draining requests.
   */
  def drain(): Future[Unit]
}
