package com.twitter.finagle.mux.exp

import com.twitter.finagle.Service
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future}

/**
 * A MuxService provides the RPC, message-passing, pinging, and
 * draining facilities for one direction of a mux session.  For both
 * connectors and listeners a Session is established by providing a
 * MuxService server implementation for receiving messages. The
 * Session in turn exposes a MuxService client for sending messages.
 */
trait MuxService extends Service[Buf, Buf] {
  def send(buf: Buf): Future[Unit]
  def ping(): Future[Unit]
  def drain(): Future[Unit]
}
