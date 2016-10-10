package com.twitter.finagle.netty4.channel

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.framer.Framer
import com.twitter.finagle.netty4.DirectToHeapInboundHandlerName
import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.finagle.netty4.framer.FrameHandler
import com.twitter.finagle.netty4.poolReceiveBuffers
import com.twitter.finagle.netty4.proxy.{HttpProxyConnectHandler, Netty4ProxyConnectHandler}
import com.twitter.finagle.netty4.ssl.Netty4SslHandler
import com.twitter.finagle.param.{Label, Logger, Stats}
import com.twitter.finagle.Stack
import com.twitter.finagle.transport.Transport
import com.twitter.util.Duration
import io.netty.channel._
import io.netty.handler.proxy.{HttpProxyHandler, Socks5ProxyHandler}
import io.netty.handler.timeout.{ReadTimeoutHandler, WriteTimeoutHandler}
import java.util.logging.Level

private[netty4] object Netty4ClientChannelInitializer {
  val BufCodecKey = "buf codec"
  val FramerKey = "framer"
  val WriteTimeoutHandlerKey = "write timeout"
  val ReadTimeoutHandlerKey = "read timeout"
  val ConnectionHandlerKey = "connection handler"
  val ChannelStatsHandlerKey = "channel stats"
  val ChannelRequestStatsHandlerKey = "channel request stats"
  val ChannelLoggerHandlerKey = "channel logger"
}

/**
 * Client channel initialization logic.
 *
 * @param params configuration parameters.
 * @param framerFactory initialize per-channel framer for emitting framed
 *                      messages.
 */
private[netty4] class Netty4ClientChannelInitializer(
    params: Stack.Params,
    framerFactory: Option[() => Framer] = None)
  extends AbstractNetty4ClientChannelInitializer(params) {
  import Netty4ClientChannelInitializer._

  override def initChannel(ch: Channel): Unit = {
    super.initChannel(ch)

    // fist => last
    // - a request flies from last to first
    // - a response flies from first to last
    //
    // [pipeline from super.initChannel] => bufCodec => framer

    val pipe = ch.pipeline

    pipe.addLast(BufCodecKey, new BufCodec)

    framerFactory.foreach { newFramer =>
      pipe.addLast(FramerKey, new FrameHandler(newFramer()))
    }
  }
}

/**
 * Base initializer which installs read / write timeouts and a connection handler
 */
private[netty4] abstract class AbstractNetty4ClientChannelInitializer(
    params: Stack.Params)
  extends ChannelInitializer[Channel] {

  import Netty4ClientChannelInitializer._

  private[this] val Transport.Liveness(readTimeout, writeTimeout, _) = params[Transport.Liveness]
  private[this] val Logger(logger) = params[Logger]
  private[this] val Label(label) = params[Label]
  private[this] val Stats(stats) = params[Stats]
  private[this] val Transporter.HttpProxyTo(httpHostAndCredentials) =
    params[Transporter.HttpProxyTo]
  private[this] val Transporter.SocksProxy(socksAddress, socksCredentials) =
    params[Transporter.SocksProxy]
  private[this] val Transporter.HttpProxy(httpAddress, httpCredentials) =
    params[Transporter.HttpProxy]

  private[this] val channelSnooper =
    if (params[Transport.Verbose].enabled)
      Some(ChannelSnooper.byteSnooper(label)(logger.log(Level.INFO, _, _)))
    else
      None

  private[this] val (channelRequestStatsHandler, channelStatsHandler) =
    if (!stats.isNull)
      (Some(new ChannelRequestStatsHandler(stats)), Some(new ChannelStatsHandler(stats)))
    else
      (None, None)

  private[this] val exceptionHandler = new ChannelExceptionHandler(stats, logger)

  def initChannel(ch: Channel): Unit = {

    // first => last
    // - a request flies from last to first
    // - a response flies from first to last
    //
    // http proxy => ssl => read timeout => write timeout => ...
    // ... => channel stats => req stats => exceptions

    val pipe = ch.pipeline

    channelStatsHandler.foreach(pipe.addFirst(ChannelStatsHandlerKey, _))
    channelSnooper.foreach(pipe.addFirst(ChannelLoggerHandlerKey, _))
    channelRequestStatsHandler.foreach(pipe.addLast(ChannelRequestStatsHandlerKey, _))

    if (readTimeout.isFinite && readTimeout > Duration.Zero) {
      val (timeoutValue, timeoutUnit) = readTimeout.inTimeUnit
      pipe.addFirst(ReadTimeoutHandlerKey, new ReadTimeoutHandler(timeoutValue, timeoutUnit))
    }

    if (writeTimeout.isFinite && writeTimeout > Duration.Zero) {
      val (timeoutValue, timeoutUnit) = writeTimeout.inTimeUnit
      pipe.addLast(WriteTimeoutHandlerKey, new WriteTimeoutHandler(timeoutValue, timeoutUnit))
    }

    pipe.addLast("exception handler", exceptionHandler)

    // Add SslHandler to the pipeline.
    pipe.addFirst("ssl init", new Netty4SslHandler(params))

    // SOCKS5 proxy via `Netty4ProxyConnectHandler`.
    socksAddress.foreach { sa =>
      val proxyHandler = socksCredentials match {
        case None => new Socks5ProxyHandler(sa)
        case Some((u, p)) => new Socks5ProxyHandler(sa, u, p)
      }

      pipe.addFirst("socks proxy connect", new Netty4ProxyConnectHandler(proxyHandler))
    }

    // HTTP proxy via `Netty4ProxyConnectHandler`.
    httpAddress.foreach { sa =>
      val proxyHandler = httpCredentials match {
        case None => new HttpProxyHandler(sa)
        case Some(c) => new HttpProxyHandler(sa, c.username, c.password)
      }

      pipe.addFirst("http proxy connect", new Netty4ProxyConnectHandler(proxyHandler))
    }

    // TCP tunneling via HTTP proxy (using `HttpProxyConnectHandler`).
    httpHostAndCredentials.foreach {
      case (host, credentials) => pipe.addFirst("http proxy connect",
        new HttpProxyConnectHandler(host, credentials))
    }

    // Copy direct byte buffers onto heap before doing anything else.
    pipe.addFirst(DirectToHeapInboundHandlerName, DirectToHeapInboundHandler)

    // Enable tracking of the receive buffer sizes (when `poolReceiveBuffers` is enabled).
    if (poolReceiveBuffers()) {
      pipe.addFirst("receive buffers size tracker",
        new RecvBufferSizeStatsHandler(stats.scope("transport")))
    }
  }
}

/**
 * Channel Initializer which exposes the netty pipeline to the transporter.
 *
 * @param params configuration parameters.
 * @param pipelineInit a callback for initialized pipelines
 */
private[netty4] class RawNetty4ClientChannelInitializer(
    pipelineInit: ChannelPipeline => Unit,
    params: Stack.Params)
  extends AbstractNetty4ClientChannelInitializer(params) {

  override def initChannel(ch: Channel): Unit = {
    super.initChannel(ch)
    pipelineInit(ch.pipeline)
  }
}
