package com.twitter.finagle.netty4.transport

import com.twitter.finagle.Status
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.{Future, Promise, Time}
import io.netty.channel.Channel
import io.netty.handler.ssl.SslHandler
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/**
 * `TransportContext` for use with a Finagle Netty4
 * `ChannelTransport`.
 */
private[finagle] final class ChannelTransportContext(ch: Channel)
    extends TransportContext
    with HasExecutor {

  // Accessible by the `ChannelTransport` and for testing.
  private[transport] val failed = new AtomicBoolean(false)
  private[transport] val closed = new Promise[Throwable]
  private[transport] val alreadyClosed = new AtomicBoolean(false)

  def status: Status =
    if (failed.get || !ch.isOpen) Status.Closed
    else Status.Open

  def onClose: Future[Throwable] = closed

  def localAddress: SocketAddress = ch.localAddress

  def remoteAddress: SocketAddress = ch.remoteAddress

  val peerCertificate: Option[Certificate] = ch.pipeline.get(classOf[SslHandler]) match {
    case null => None
    case handler =>
      try {
        handler.engine.getSession.getPeerCertificates.headOption
      } catch {
        case NonFatal(_) => None
      }
  }

  def close(deadline: Time): Future[Unit] = {
    // we check if this has already been closed because of a netty bug
    // https://github.com/netty/netty/issues/7638.  Remove this work-around once
    // it's fixed.
    if (alreadyClosed.compareAndSet(false, true) && ch.isOpen) ch.close()
    closed.unit
  }

  def executor: Executor = ch.eventLoop
}
