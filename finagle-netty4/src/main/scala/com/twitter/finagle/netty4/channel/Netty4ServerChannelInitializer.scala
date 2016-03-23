package com.twitter.finagle.netty4.channel

import java.util.logging.Level

import com.twitter.finagle.netty4.Netty4ListenerTLSConfig
import com.twitter.finagle.netty4.ssl.TlsShutdownHandler
import com.twitter.finagle.param._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Stack
import io.netty.channel._
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.socket.SocketChannel
import io.netty.handler.ssl.SslHandler
import io.netty.handler.timeout._

private[netty4] object Netty4ServerChannelInitializer {
  val ChannelLoggerHandlerKey = "channel logger"
  val ChannelStatsHandlerKey = "channel stats"
  val WriteTimeoutHandlerKey = "write timeout"
  val ReadTimeoutHandlerKey = "read timeout"
}

/**
 * Server channel initialization logic.
 *
 * @param pipelineInit Initializes a pipeline for encoding input
 *                     messages and decoding output messages on
 *                     accepted channels.
 * @param params [[Stack.Params]] to configure the channel.
 * @param newBridge A [[io.netty.channel.ChannelHandler]] to bridge the
 *                  netty message pipeline to a [[Transport]].
 */
private[netty4] class Netty4ServerChannelInitializer(
    pipelineInit: ChannelPipeline => Unit,
    params: Stack.Params,
    newBridge: () => ChannelHandler)
  extends ChannelInitializer[SocketChannel] {

  import Netty4ServerChannelInitializer._

  private[this] val Logger(logger) = params[Logger]
  private[this] val Label(label) = params[Label]
  private[this] val Transport.Liveness(readTimeout, writeTimeout, _) = params[Transport.Liveness]
  private[this] val Stats(stats) = params[Stats]

  private[this] val (channelRequestStatsHandler, channelStatsHandler) =
    if (!stats.isNull)
      (Some(new ChannelRequestStatsHandler(stats)), Some(new ChannelStatsHandler(stats)))
    else
      (None, None)

  private[this] val channelSnooper =
    if (params[Transport.Verbose].enabled)
      Some(ChannelSnooper(label)(logger.log(Level.INFO, _, _)))
    else
      None

  val Transport.TLSServerEngine(engine) = params[Transport.TLSServerEngine]
  val tlsConfig = engine.map(Netty4ListenerTLSConfig)

  private[this] val exceptionHandler = new ChannelExceptionHandler(stats, logger)

  def initChannelTls(config: Netty4ListenerTLSConfig, ch: SocketChannel): Unit = {
    for (Netty4ListenerTLSConfig(newEngine) <- tlsConfig){
      val engine = newEngine()
      engine.self.setUseClientMode(false)
      engine.self.setEnableSessionCreation(true)
      val handler = new SslHandler(engine.self)
      // todo: verify renegotiation works with jsse and openssl (CSL-1973)
      ch.pipeline.addFirst("ssl", handler)

      ch.pipeline.addFirst(
        "sslShutdown",
        new TlsShutdownHandler(engine)
      )
    }
  }

  def initChannel(ch: SocketChannel): Unit = {

    // last => first (an incoming request flies from left to right)
    // read timeout => write timeout => ssl => stats => snooper => req stats => exceptions => bridge => ct

    val pipeline = ch.pipeline
    pipelineInit(pipeline)

    channelSnooper.foreach(pipeline.addFirst(ChannelLoggerHandlerKey, _))
    channelStatsHandler.foreach(pipeline.addFirst(ChannelStatsHandlerKey, _))

    if (writeTimeout.isFinite) {
      val (timeoutValue, timeoutUnit) = writeTimeout.inTimeUnit
      pipeline.addLast(WriteTimeoutHandlerKey, new WriteTimeoutHandler(timeoutValue, timeoutUnit))
    }

    tlsConfig.foreach(initChannelTls(_, ch))

    if (readTimeout.isFinite) {
      val (timeoutValue, timeoutUnit) = readTimeout.inTimeUnit
      pipeline.addLast(ReadTimeoutHandlerKey, new ReadTimeoutHandler(timeoutValue, timeoutUnit))
    }

    channelRequestStatsHandler.foreach(pipeline.addLast("channel request stats handler", _))

    pipeline.addLast("exception handler", exceptionHandler)

    // The bridge handler must be last in the pipeline to ensure
    // that the bridging code sees all encoding and transformations
    // of inbound messages.
    pipeline.addLast("finagle bridge", newBridge())
  }
}

/**
 * Bridges a channel onto a transport.
 */
@Sharable
private[netty4] class ServerBridge[In, Out](
    transportFac: SocketChannel => Transport[In, Out],
    serveTransport: Transport[In, Out] => Unit)
  extends ChannelInboundHandlerAdapter {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val transport: Transport[In, Out] = transportFac(ctx.channel.asInstanceOf[SocketChannel])
    serveTransport(transport)
    super.channelActive(ctx)
  }
}
