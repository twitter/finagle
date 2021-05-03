package com.twitter.finagle.http2.transport.client

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.MultiplexHandlerBuilder
import com.twitter.finagle.http2.transport.common.H2StreamChannelInit
import com.twitter.finagle.netty4.http._
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.netty4.{ConnectionBuilder, Netty4Transporter}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.{Future, Return, Throw}
import io.netty.channel.{Channel, ChannelPipeline}
import io.netty.handler.ssl.{ApplicationProtocolNames, SslHandler}
import java.net.SocketAddress

/**
 * Establishes connections to the peer using TLS and attempting to negotiate the H2 protocol
 *
 * This `Transporter` follows a similar model to the `H2CTransporter` in terms of propagating
 * the H2 session to the pooling layer. First, a connection is requested. As part of the TLS
 * establishment ALPN is used to negotiate the HTTP protocol with preference for HTTP/2. if
 * HTTP/2 is negotiated, the first stream is propagated up in the traditional style to the
 * dispatcher layer while the rest of the session is sent to the pool via the `OnH2Service`
 * passed in via the params.
 */
private[http2] class TlsTransporter private (
  connectionBuilder: ConnectionBuilder,
  modifier: Transport[Any, Any] => Transport[Any, Any],
  params: Stack.Params)
    extends Transporter[Any, Any, TransportContext] {

  import TlsTransporter._

  private[this] val statsReceiver = params[Stats].statsReceiver
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")
  private[this] val onH2Service = params[H2Pool.OnH2ServiceParam].onH2Service match {
    case Some(s) => s
    case None =>
      throw new IllegalStateException(
        s"params are missing the ${classOf[H2Pool.OnH2Service].getSimpleName}")
  }

  def remoteAddress: SocketAddress = connectionBuilder.remoteAddress

  /** Attempt to upgrade to a multiplex session */
  def apply(): Future[Transport[Any, Any]] =
    connectionBuilder.build { channel =>
      val sslHandler = channel.pipeline.get(classOf[SslHandler])
      val proto = sslHandler.applicationProtocol
      val transport = onConnect(channel, if (proto == null) DefaultProtocol else proto)
      // remove the DelayByteBufHandler to release the inbound ByteBuf
      channel.pipeline.remove(classOf[DelayByteBufHandler])
      transport
    }

  private[this] def onConnect(channel: Channel, protocol: String): Future[Transport[Any, Any]] = {
    protocol match {
      case ApplicationProtocolNames.HTTP_2 =>
        handleH2Upgrade(channel)

      case ApplicationProtocolNames.HTTP_1_1 =>
        Future.value(configureHttp1Pipeline(channel, params))

      case _ =>
        channel.close()
        throw new IllegalStateException("unknown protocol: " + protocol)
    }
  }

  private[this] def handleH2Upgrade(channel: Channel): Future[Transport[Any, Any]] = {
    upgradeCounter.incr()
    val session = configureHttp2Pipeline(channel, params)
    val childTransport = session.newChildTransport().map(new SingleDispatchTransport(_))

    childTransport.respond {
      case Return(t) =>
        val dSession = new DeferredCloseSession(session, t.onClose.unit)
        onH2Service(new ClientServiceImpl(dSession, statsReceiver, modifier))

      case Throw(_) =>
        // If we can't get a stream we're almost certainly already closed.
        session.close()
    }

    childTransport
  }
}

object TlsTransporter {

  private val DefaultProtocol = ApplicationProtocolNames.HTTP_1_1

  // add the DelayByteBufHandler to hold the inbound ByteBuf
  private[this] val addDelayer: ChannelPipeline => Unit = { pipeline =>
    pipeline.addLast("delayByteBufHandler", new DelayByteBufHandler)
  }

  def make(
    addr: SocketAddress,
    modifier: Transport[Any, Any] => Transport[Any, Any],
    params: Stack.Params
  ): Transporter[Any, Any, TransportContext] = {
    val connectionBuilder = {
      // For the initial TLS handshake and MultiplexCodec handler we don't want back pressure
      // so we disable it for now. If we end up with a HTTP/1.x session we will honor the
      // settings specified in the params when reconfiguring as a HTTP/1.x pipeline.
      ConnectionBuilder.rawClient(
        addDelayer,
        addr,
        params + Netty4Transporter.Backpressure(false)
      )
    }

    new TlsTransporter(connectionBuilder, modifier, params)
  }

  private def configureHttp2Pipeline(channel: Channel, params: Stack.Params): ClientSession = {
    val (codec, handler) = MultiplexHandlerBuilder.clientFrameCodec(params, None)
    val streamChannelInit = H2StreamChannelInit.initClient(params)
    val pingDetectionHandler = new H2ClientFilter(params)
    MultiplexHandlerBuilder.addStreamsGauge(params[Stats].statsReceiver, codec, channel)

    channel.pipeline
      .addLast(Http2CodecName, codec)
      .addLast(Http2MultiplexHandlerName, handler)
      .addLast(H2ClientFilter.HandlerName, pingDetectionHandler)

    new ClientSessionImpl(params, streamChannelInit, channel, () => pingDetectionHandler.status)
  }

  private def configureHttp1Pipeline(
    channel: Channel,
    params: Stack.Params
  ): Transport[Any, Any] = {
    val pipeline = channel.pipeline
    pipeline.addLast(HttpCodecName, newHttpClientCodec(params))
    initClient(params)(pipeline)

    // We've found ourselves with a HTTP/1.x connection so we need to configure the
    // socket pipeline with back pressure to whatever the params say. Unfortunately,
    // we need to make sure to properly invert the boolean since
    // auto read means no backpressure.
    val autoRead = !params[Netty4Transporter.Backpressure].backpressure
    pipeline.channel.config.setAutoRead(autoRead)

    // This is a traditional channel, so we want to keep the stack traces.
    new ChannelTransport(channel, new AsyncQueue[Any], omitStackTraceOnInactive = false)
  }
}
