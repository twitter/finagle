package com.twitter.finagle.netty4

import com.twitter.finagle.Stack
import com.twitter.finagle.codec.{FrameDecoder, FrameEncoder}
import com.twitter.finagle.netty4.channel.WriteCompletionTimeoutHandler
import com.twitter.finagle.netty4.codec.{EncodeHandler, DecodeHandler}
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.Timer
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Return, Promise}
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.timeout.ReadTimeoutHandler

/**
 * Client channel initialization logic.
 *
 * @param transportP the [[Promise]] satisfied when a connected transport is created.
 * @param params configuration parameters.
 * @param encoder serialize an [[In]]-typed application message.
 * @param decoderFactory initialize per-channel deserializer for
 *                       emitting [[Out]]-typed messages.
 * @param transportFactory applied per connected channel.
 * @tparam In the application request type.
 * @tparam Out the application response type.
 */
private[netty4] class Netty4ClientChannelInitializer[In, Out](
    transportP: Promise[Transport[In, Out]],
    params: Stack.Params,
    encoder: Option[FrameEncoder[In]],
    decoderFactory: Option[() => FrameDecoder[Out]],
    transportFactory: Channel => Transport[In, Out] = new ChannelTransport[In, Out](_))
  extends ChannelInitializer[SocketChannel] {

  private[this] val encodeHandler = encoder.map(new EncodeHandler[In](_))
  private[this] val decodeHandler = decoderFactory.map(new DecodeHandler[Out](_))
  private[this] val Timer(timer) = params[Timer]
  private[this] val Transport.Liveness(readTimeout, writeTimeout, _) = params[Transport.Liveness]

  def initChannel(ch: SocketChannel): Unit = {

    // read timeout => decode handler => encode handler => write timeout => cxn handler
    val pipe = ch.pipeline
    decodeHandler.foreach(pipe.addFirst("frame decoder", _))

    if (readTimeout.isFinite) {
      val (timeoutValue, timeoutUnit) = readTimeout.inTimeUnit
      pipe.addFirst("read timeout", new ReadTimeoutHandler(timeoutValue, timeoutUnit))
    }

    encodeHandler.foreach(pipe.addLast("frame encoder", _))

    if (writeTimeout.isFinite)
      pipe.addLast("write timeout", new WriteCompletionTimeoutHandler(timer, writeTimeout))

    pipe.addLast("connection handler", new ConnectionHandler(transportP, transportFactory))
  }
}

private[netty4] class ConnectionHandler[In, Out](
    p: Promise[Transport[In,Out]],
    transportFac: Channel => Transport[In, Out] = new ChannelTransport[In, Out](_))
  extends ChannelInboundHandlerAdapter {

  override def isSharable = false

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    p.updateIfEmpty(Return(transportFac(ctx.channel)))
    super.channelActive(ctx)
  }
}
