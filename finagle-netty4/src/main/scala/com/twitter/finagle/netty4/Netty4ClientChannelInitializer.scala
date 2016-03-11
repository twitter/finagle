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

private[netty4] object Netty4ClientChannelInitializer {
  val FrameDecoderHandlerKey = "frame decoder"
  val FrameEncoderHandlerKey = "frame encoder"
  val WriteTimeoutHandlerKey = "write timeout"
  val ReadTimeoutHandlerKey = "read timeout"
  val ConnectionHandlerKey = "connection handler"
}

/**
 * Client channel initialization logic.
 *
 * @param transportP the [[Promise]] satisfied when a connected transport is created.
 * @param params configuration parameters.
 * @param encoder serialize an [[In]]-typed application message.
 * @param decoderFactory initialize per-channel deserializer for
 *                       emitting [[Out]]-typed messages.
 * @tparam In the application request type.
 * @tparam Out the application response type.
 */
private[netty4] class Netty4ClientChannelInitializer[In, Out](
    transportP: Promise[Transport[In, Out]],
    params: Stack.Params,
    encoder: Option[FrameEncoder[In]] = None,
    decoderFactory: Option[() => FrameDecoder[Out]] = None)
  extends AbstractNetty4ClientChannelInitializer[In, Out](transportP, params) {
  import Netty4ClientChannelInitializer._

  private[this] val encodeHandler = encoder.map(new EncodeHandler[In](_))
  private[this] val decodeHandler = decoderFactory.map(new DecodeHandler[Out](_))

  override def initChannel(ch: SocketChannel): Unit = {
    super.initChannel(ch)

    // read timeout => decode handler => encode handler => write timeout => cxn handler
    val pipe = ch.pipeline
    decodeHandler.foreach(pipe.addFirst(FrameDecoderHandlerKey, _))

    encodeHandler.foreach { enc =>
      if (pipe.get(WriteTimeoutHandlerKey) != null)
        pipe.addBefore(WriteTimeoutHandlerKey, FrameEncoderHandlerKey, enc)
      else
        pipe.addLast(FrameEncoderHandlerKey, enc)
    }
  }
}

/**
 * Base initializer which installs read / write timeouts and a connection handler
 */
private[netty4] abstract class AbstractNetty4ClientChannelInitializer[In, Out](
    transportP: Promise[Transport[In, Out]],
    params: Stack.Params)
  extends ChannelInitializer[SocketChannel] {
    import Netty4ClientChannelInitializer._
    private[this] val Timer(timer) = params[Timer]
    private[this] val Transport.Liveness(readTimeout, writeTimeout, _) = params[Transport.Liveness]

    def initChannel(ch: SocketChannel): Unit = {
      val pipe = ch.pipeline

      if (readTimeout.isFinite) {
        val (timeoutValue, timeoutUnit) = readTimeout.inTimeUnit
        pipe.addFirst(ReadTimeoutHandlerKey, new ReadTimeoutHandler(timeoutValue, timeoutUnit))
      }

      if (writeTimeout.isFinite)
        pipe.addLast(WriteTimeoutHandlerKey, new WriteCompletionTimeoutHandler(timer, writeTimeout))

      pipe.addLast(ConnectionHandlerKey, new ConnectionHandler(transportP))
    }
  }

/**
 * Channel Initializer which exposes the netty pipeline to the transporter.
 * @param transportP the [[Promise]] satisfied when a connected transport is created.
 * @param params configuration parameters.
 * @param pipeCb a callback for initialized pipelines
 * @tparam In the application request type.
 * @tparam Out the application response type.
 */
private[netty4] class RawNetty4ClientChannelInitializer[In, Out](
    transportP: Promise[Transport[In, Out]],
    params: Stack.Params,
    pipeCb: ChannelPipeline => Unit)
  extends AbstractNetty4ClientChannelInitializer[In, Out](transportP, params) {

  override def initChannel(ch: SocketChannel): Unit = {
    super.initChannel(ch)
    pipeCb(ch.pipeline)
  }
}

private[netty4] class ConnectionHandler[In, Out](
    p: Promise[Transport[In,Out]])
  extends ChannelInboundHandlerAdapter {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val transport = new ChannelTransport[In, Out](ctx.channel)
    p.updateIfEmpty(Return(transport))

    // we don't reuse channels so we remove the connection handler
    // after the first activation to shorten the handler pipeline.
    ctx.pipeline.remove(this)
    super.channelActive(ctx)
  }
}
