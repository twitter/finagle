package com.twitter.finagle.http2.exp.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.MultiplexCodecBuilder
import com.twitter.finagle.http2.transport.{
  ClientSession,
  H2Filter,
  H2StreamChannelInit,
  Http2NegotiatingTransporter
}
import com.twitter.finagle.netty4.http.{
  Http2CodecName,
  HttpCodecName,
  Netty4HttpTransporter,
  initClient,
  newHttpClientCodec
}
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.netty4.{ConnectionBuilder, Netty4Transporter}
import com.twitter.finagle.param.{Stats, Timer}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.Stack
import com.twitter.util.Future
import io.netty.channel.{Channel, ChannelPipeline}
import io.netty.handler.ssl.{ApplicationProtocolNames, SslHandler}
import java.net.SocketAddress

/**
 * Behavior characteristics:
 *
 * New sessions will wait for the status of the negotiation of the first one to see
 * if we have a HTTP/2 session.
 *
 * If we fail to negotiate HTTP/2 we revert to HTTP/1 behavior and don't attempt to
 * upgrade anymore. (why? it's reasonably cheap since it's part of the TLS handshake.)
 */
private[http2] class TlsTransporter(
  connectionBuilder: ConnectionBuilder,
  params: Stack.Params,
  underlyingHttp11: Transporter[Any, Any, TransportContext])
    extends Http2NegotiatingTransporter(
      params,
      underlyingHttp11,
      fallbackToHttp11WhileNegotiating = false
    ) {

  import TlsTransporter._

  private[this] val statsReceiver = params[Stats].statsReceiver
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  /** Attempt to upgrade to a multiplex session */
  protected def attemptUpgrade(): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
    val f = connectionBuilder.build { channel =>
      val sslHandler = channel.pipeline.get(classOf[SslHandler])
      val proto = sslHandler.applicationProtocol
      onConnect(channel, if (proto == null) DefaultProtocol else proto)
    }

    val sessionF = f.map {
      case Right(_) => None
      case Left(clientSession) => Some(clientSession)
    }

    val transportF = f.flatMap {
      case Right(transport) => Future.value(transport)
      case Left(clientSession) => clientSession.newChildTransport()
    }

    sessionF -> transportF
  }

  private[this] def onConnect(
    channel: Channel,
    protocol: String
  ): Future[Either[ClientSession, Transport[Any, Any]]] = Future {
    protocol match {
      case ApplicationProtocolNames.HTTP_2 =>
        val session = configureHttp2Pipeline(channel, params)
        upgradeCounter.incr()
        Left(session)

      case ApplicationProtocolNames.HTTP_1_1 =>
        Right(configureHttp1Pipeline(channel, params))

      case _ =>
        channel.close()
        throw new IllegalStateException("unknown protocol: " + protocol)
    }
  }
}

object TlsTransporter {

  private val DefaultProtocol = ApplicationProtocolNames.HTTP_1_1

  def make(addr: SocketAddress, params: Stack.Params): Transporter[Any, Any, TransportContext] = {
    val connectionBuilder = {
      // current http2 client implementation doesn't support
      // netty-style backpressure
      // https://github.com/netty/netty/issues/3667#issue-69640214
      val withBackpressure = params + Netty4Transporter.Backpressure(false)
      ConnectionBuilder.rawClient(
        init(withBackpressure),
        addr,
        withBackpressure
      )
    }

    val underlyingHttp11 = Netty4HttpTransporter(params)(addr)
    new TlsTransporter(connectionBuilder, params, underlyingHttp11)
  }

  private def init(params: Stack.Params)(pipeline: ChannelPipeline): Unit = {
    // nop
  }

  def configureHttp2Pipeline(channel: Channel, params: Stack.Params): ClientSession = {
    val multiplex = MultiplexCodecBuilder.clientMultiplexCodec(params, None)
    val streamChannelInit = H2StreamChannelInit.initClient(params)
    channel.pipeline.addLast(Http2CodecName, multiplex)
    channel.pipeline.addLast(H2Filter.HandlerName, new H2Filter(params[Timer].timer))
    new ClientSessionImpl(params, streamChannelInit, channel)
  }

  def configureHttp1Pipeline(channel: Channel, params: Stack.Params): Transport[Any, Any] = {
    val pipeline = channel.pipeline
    pipeline.addLast(HttpCodecName, newHttpClientCodec(params))
    initClient(params)(pipeline)
    pipeline.channel.config.setAutoRead(false)
    // This is a traditional channel, so we want to keep the stack traces.
    new ChannelTransport(channel, new AsyncQueue[Any], omitStackTraceOnInactive = false)
  }
}
