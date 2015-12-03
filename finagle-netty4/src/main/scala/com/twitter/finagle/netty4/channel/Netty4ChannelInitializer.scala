package com.twitter.finagle.netty4.channel

import java.util.logging.Level

import com.twitter.finagle.netty4.Netty4ListenerTLSConfig
import com.twitter.finagle.netty4.ssl.TlsShutdownHandler
import com.twitter.finagle.param._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Failure, Stack, WriteTimedOutException}
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.ssl.SslHandler
import io.netty.handler.timeout._

/**
 *
 * @param pipelineInit Initializes a pipeline for encoding input
 *                     messages and decoding output messages on
 *                     accepted channels.
 * @param params [[Stack.Params]] to configure the channel.
 * @param newBridge A [[io.netty.channel.ChannelHandler]] to bridge the
 *                  netty message pipeline to a [[Transport]].
 */
private[netty4] class Netty4ChannelInitializer(
    pipelineInit: ChannelPipeline => Unit,
    params: Stack.Params,
    newBridge: () => ChannelHandler)
  extends ChannelInitializer[SocketChannel] {

  val Logger(logger) = params[Logger]
  val Label(label) = params[Label]
  val Transport.Liveness(readTimeout, writeTimeout, keepAlive) = params[Transport.Liveness]
  val Timer(timer) = params[Timer]
  val Stats(stats) = params[Stats]

  val writeCompletionTimeoutHandler =
    if (writeTimeout.isFinite)
      Some(new WriteCompletionTimeoutHandler(timer, writeTimeout))
    else
      None

  val (channelRequestStatsHandler, channelStatsHandler) =
    if (!stats.isNull)
      (Some(new ChannelRequestStatsHandler(stats)), Some(new ChannelStatsHandler(stats)))
    else
      (None, None)

  val channelSnooper =
    if (params[Transport.Verbose].enabled)
      Some(ChannelSnooper(label)(logger.log(Level.INFO, _, _)))
    else
      None

  val Transport.TLSServerEngine(engine) = params[Transport.TLSServerEngine]
  val tlsConfig = engine.map(Netty4ListenerTLSConfig)

  val exceptionHandler = new ChannelExceptionHandler(stats, logger)

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
    val pipeline = ch.pipeline
    pipelineInit(pipeline)

    channelSnooper.foreach(pipeline.addFirst("channelLogger", _))
    channelStatsHandler.foreach(pipeline.addFirst("channelStatsHandler", _))
    writeCompletionTimeoutHandler.foreach(pipeline.addLast("writeCompletionTimeout", _))

    if (readTimeout.isFinite) {
      val (timeoutValue, timeoutUnit) = readTimeout.inTimeUnit
      pipeline.addLast("readTimeout", new ReadTimeoutHandler(timeoutValue, timeoutUnit))
    }

    tlsConfig.foreach(initChannelTls(_, ch))

    channelRequestStatsHandler.foreach(pipeline.addLast("channelRequestStatsHandler", _))

    pipeline.addLast("exceptionHandler", exceptionHandler)
    // The bridge handler must be last in the pipeline to ensure
    // that the bridging code sees all encoding and transformations
    // of inbound messages.
    pipeline.addLast("finagleBridge", newBridge())
  }
}

/**
 * Bridges a channel onto a transport.
 */
private[netty4] class ServerBridge[In, Out](
    transportFac: SocketChannel => Transport[In, Out],
    serveTransport: Transport[In, Out] => Unit)
  extends ChannelInboundHandlerAdapter {

  override def isSharable = true

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val transport: Transport[In, Out] = transportFac(ctx.channel.asInstanceOf[SocketChannel])
    serveTransport(transport)
    super.channelActive(ctx)
  }
}

private[netty4] object ChannelExceptionHandler {
  private val FinestIOExceptionMessages = Set(
    "Connection reset by peer",
    "Broken pipe",
    "Connection timed out",
    "No route to host",
    "")
}


/**
 * Logs channel exceptions
 */
private[netty4] class ChannelExceptionHandler(
    stats: StatsReceiver,
    log: java.util.logging.Logger)
  extends ChannelInboundHandlerAdapter {
  import ChannelExceptionHandler.FinestIOExceptionMessages

  override def isSharable = true

  private[this] val readTimeoutCounter = stats.counter("read_timeout")
  private[this] val writeTimeoutCounter = stats.counter("write_timeout")

  private[this] def severity(exc: Throwable): Level = exc match {
    case e: Failure => e.logLevel
    case
      _: java.nio.channels.ClosedChannelException
      | _: javax.net.ssl.SSLException
      | _: ReadTimeoutException
      | _: WriteTimedOutException
      | _: javax.net.ssl.SSLException => Level.FINEST
    case e: java.io.IOException if FinestIOExceptionMessages.contains(e.getMessage) =>
      Level.FINEST
    case _ => Level.WARNING
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, t: Throwable): Unit = {
    t match {
      case e: ReadTimeoutException => readTimeoutCounter.incr()
      case e: WriteTimedOutException => writeTimeoutCounter.incr()
      case _ =>
    }

    val remoteAddr =
      Option(ctx.channel.remoteAddress).map(_.toString).getOrElse("unknown remote address")
    val msg = s"Unhandled exception in connection with $remoteAddr, shutting down connection"

    log.log(severity(t), msg, t)

    if (ctx.channel.isOpen)
      ctx.channel.close()

    super.exceptionCaught(ctx, t)
  }
}
