package com.twitter.finagle.http2.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.Http2Transporter
import com.twitter.finagle.http2.transport.Http2ClientDowngrader.StreamMessage
import com.twitter.finagle.param.Stats
import com.twitter.finagle.Stack
import com.twitter.finagle.transport.Transport
import com.twitter.logging.Logger
import com.twitter.util.{Future, ConstFuture, Promise}
import java.util.concurrent.atomic.AtomicReference
import java.net.SocketAddress

/**
 * This `Transporter` makes `Transports` that speak netty http/1.1, but writes
 * http/2 to the wire.  It also caches a connection per address so that it can
 * be multiplexed under the hood.
 *
 * It doesn't attempt to do an http/1.1 upgrade, and has no ability to downgrade
 * to http/1.1 over the wire if the remote server doesn't speak http/2.
 * Instead, it speaks http/2 from birth.
 */
private[http2] class PriorKnowledgeTransporter(
    underlying: Transporter[Any, Any],
    params: Stack.Params)
  extends Transporter[Any, Any] {

  private[this] val log = Logger.get()
  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  def remoteAddress: SocketAddress = underlying.remoteAddress

  private[this] val ref = new AtomicReference[Future[MultiplexedTransporter]](null)

  private[this] def createMultiplexedTransporter(): Future[MultiplexedTransporter] = {
    underlying().map { transport =>
      val multi = new MultiplexedTransporter(
        Transport.cast[StreamMessage, StreamMessage](transport),
        remoteAddress
      )
      transport.onClose.ensure {
        ref.set(null)
      }

      // Consider the creation of a new prior knowledge transport as an upgrade
      upgradeCounter.incr()
      multi
    }
  }

  private[this] def getMulti(): Future[MultiplexedTransporter] = {
    Option(ref.get) match {
      case None =>
        val p = Promise[MultiplexedTransporter]()
        if (ref.compareAndSet(null, p)) {
          p.become(createMultiplexedTransporter())
          p
        } else getMulti()
      case Some(f) => f
    }
  }

  def apply(): Future[Transport[Any, Any]] = getMulti().flatMap { multi =>
    new ConstFuture(multi().map(Http2Transporter.unsafeCast)).rescue { case exn: Throwable =>
      log.warning(
        exn,
        s"A previously successful connection to address $remoteAddress stopped being successful."
      )

      ref.set(null)
      apply()
    }
  }
}
