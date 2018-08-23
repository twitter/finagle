package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.stats.{StatsReceiver, Verbosity}
import com.twitter.util.Stopwatch
import io.netty.channel.{Channel, ChannelHandlerContext}
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.{Future => NettyFuture, GenericFutureListener}

/**
 * A Finagle-specific extension of Netty's `SslHandler`. This class provides
 * an opportunity to add Finagle-specific stats, logging, and exception handling
 * for SSL/TLS traffic.
 */
private[netty4] class Netty4SslHandler(engine: Engine, statsReceiver: StatsReceiver)
  extends SslHandler(engine.self) {

  private[this] val handshakeLatency = statsReceiver.stat("handshake_latency_ms")
  private[this] val failedHandshakeLatency = statsReceiver.stat(Verbosity.Debug,
    "failed_handshake_latency_ms")

  // This is for standard TLS
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    trackHandshakeLatency()
    super.channelActive(ctx)
  }

  // This is for opportunistic TLS
  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    if (ctx.channel().isActive()) {
      trackHandshakeLatency()
    }
    super.handlerAdded(ctx)
  }

  // Add a listener so we are notified when handshake succeeds or fails.
  // This listener is added and notified AFTER the one in the verification listeners.
  private[this] def trackHandshakeLatency(): Unit = {
    val elapsed = Stopwatch.start()
    super.handshakeFuture().addListener(new GenericFutureListener[NettyFuture[Channel]] {
      override def operationComplete(f: NettyFuture[Channel]): Unit = {
        val duration = elapsed().inMilliseconds
        if (f.isSuccess) handshakeLatency.add(duration)
        else failedHandshakeLatency.add(duration)
      }
    })
  }
}

