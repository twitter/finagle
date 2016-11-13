package com.twitter.finagle.http2.transport

import com.twitter.cache.FutureCache
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.Http2Transporter
import com.twitter.finagle.http2.transport.Http2ClientDowngrader.StreamMessage
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap

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
    underlying: Transporter[Any, Any])
  extends Transporter[Any, Any] {

  private[this] val cache = new ConcurrentHashMap[SocketAddress, Future[MultiplexedTransporter]]()

  private[this] val fn: SocketAddress => Future[MultiplexedTransporter] = { addr: SocketAddress =>
    underlying(addr).map { transport =>
      val multi = new MultiplexedTransporter(
        Transport.cast[StreamMessage, StreamMessage](transport),
        addr
      )
      multi.onClose.ensure {
        cache.remove(addr, multi)
      }
      multi
    }
  }

  private[this] val cachedFn = FutureCache.fromMap(fn, cache)

  def apply(addr: SocketAddress): Future[Transport[Any, Any]] =
    cachedFn(addr).map { multi => Http2Transporter.unsafeCast(multi()) }
}
