package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.exp.transport.PriorKnowledgeTransporter.InnertHttp1Transporter
import com.twitter.finagle.http2.transport.{
  ClientSession,
  H2Filter,
  H2StreamChannelInit,
  Http2NegotiatingTransporter
}
import com.twitter.finagle.http2.MultiplexCodecBuilder
import com.twitter.finagle.netty4.ConnectionBuilder
import com.twitter.finagle.netty4.http.Http2CodecName
import com.twitter.finagle.param.{Stats, Timer}
import com.twitter.finagle.Stack
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.Future
import io.netty.channel.Channel
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
private final class PriorKnowledgeTransporter private (
  remoteAddress: SocketAddress,
  params: Stack.Params
) extends Http2NegotiatingTransporter(
      params,
      new InnertHttp1Transporter(remoteAddress),
      fallbackToHttp11WhileNegotiating = false
    ) {
  self =>

  private[this] val connectionBuilder =
    ConnectionBuilder.rawClient(_ => (), remoteAddress, params)

  private[this] val childInit = H2StreamChannelInit.initClient(params)
  private[this] val statsReceiver = params[Stats].statsReceiver
  private[this] val upgradeCounter = statsReceiver.counter("upgrade", "success")

  protected def attemptUpgrade(): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
    val clientSession = newSession()
    clientSession.map(Some(_)) -> clientSession.flatMap(_.newChildTransport())
  }

  private[this] def newSession(): Future[ClientSession] = connectionBuilder.build { channel =>
    Future(initH2SocketChannel(channel))
  }

  private[this] def initH2SocketChannel(channel: Channel): ClientSession = {
    upgradeCounter.incr()
    channel.config.setAutoRead(true) // Needs to be on for h2
    val codec = MultiplexCodecBuilder.clientMultiplexCodec(params, None)
    MultiplexCodecBuilder.addStreamsGauge(statsReceiver, codec, channel)

    channel.pipeline.addLast(Http2CodecName, codec)
    channel.pipeline.addLast(H2Filter.HandlerName, new H2Filter(params[Timer].timer))
    new ClientSessionImpl(params, childInit, channel)
  }
}

private[http2] object PriorKnowledgeTransporter {
  def make(addr: SocketAddress, params: Stack.Params): Transporter[Any, Any, TransportContext] =
    new PriorKnowledgeTransporter(addr, params)

  // We don't actually have a HTTP/1.x Transporter since this is prior knowledge so we
  // use this to make sure that we don't accidentally make the wrong kind of sessions.
  private class InnertHttp1Transporter(val remoteAddress: SocketAddress)
      extends Transporter[Any, Any, TransportContext] {
    def apply(): Future[
      Transport[Any, Any] {
        type Context <: TransportContext
      }
    ] = throw new IllegalStateException("Cannot use this Transporter")
  }
}
