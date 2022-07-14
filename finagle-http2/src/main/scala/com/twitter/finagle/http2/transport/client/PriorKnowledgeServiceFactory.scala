package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http2.MultiplexHandlerBuilder
import com.twitter.finagle.http2.transport.common.H2StreamChannelInit
import com.twitter.finagle.netty4.ConnectionBuilder
import com.twitter.finagle.netty4.http.Http2CodecName
import com.twitter.finagle.netty4.http.Http2MultiplexHandlerName
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.Status
import com.twitter.util.Future
import com.twitter.util.Time
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
private[finagle] final class PriorKnowledgeServiceFactory(
  remoteAddress: SocketAddress,
  modifier: Transport[Any, Any] => Transport[Any, Any],
  params: Stack.Params)
    extends ServiceFactory[Request, Response] { self =>

  private[this] val connectionBuilder =
    ConnectionBuilder.rawClient(_ => (), remoteAddress, params)

  private[this] val childInit = H2StreamChannelInit.initClient(params)
  private[this] val statsReceiver = params[Stats].statsReceiver
  private[this] val upgradeCounter = statsReceiver.counter("upgrade", "success")

  def apply(conn: ClientConnection): Future[Service[Request, Response]] = {
    connectionBuilder.build { channel =>
      Future.value(new ClientServiceImpl(initH2SocketChannel(channel), statsReceiver, modifier))
    }
  }

  def close(deadline: Time): Future[Unit] = Future.Done

  def status: Status = Status.Open

  private[this] def initH2SocketChannel(parentChannel: Channel): ClientSession = {
    upgradeCounter.incr()
    // By design, the MultiplexCodec handler needs the socket channel to have auto-read enabled.
    // The stream channels are configured appropriately via the params in the `ClientSessionImpl`.
    parentChannel.config.setAutoRead(true) // Needs to be on for h2
    val (codec, handler) = MultiplexHandlerBuilder.clientFrameCodec(params, None)

    MultiplexHandlerBuilder.addStreamsGauge(statsReceiver, codec, parentChannel)
    val pingDetectionHandler = new H2ClientFilter(params)

    parentChannel.pipeline
      .addLast(Http2CodecName, codec)
      .addLast(Http2MultiplexHandlerName, handler)
      .addLast(H2ClientFilter.HandlerName, pingDetectionHandler)

    new ClientSessionImpl(params, childInit, parentChannel, () => pingDetectionHandler.status)
  }
}
