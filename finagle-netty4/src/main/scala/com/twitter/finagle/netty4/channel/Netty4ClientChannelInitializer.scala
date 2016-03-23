package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Stack
import com.twitter.finagle.codec.{FrameDecoder, FrameEncoder}
import com.twitter.finagle.netty4.codec.{DecodeHandler, EncodeHandler}
import com.twitter.finagle.param.{Stats, Logger}
import com.twitter.finagle.transport.Transport
import io.netty.channel._
import io.netty.handler.timeout.{ReadTimeoutHandler, WriteTimeoutHandler}

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
 * @param params configuration parameters.
 * @param encoder serialize an [[In]]-typed application message.
 * @param decoderFactory initialize per-channel deserializer for
 *                       emitting [[Out]]-typed messages.
 * @tparam In the application request type.
 * @tparam Out the application response type.
 */
private[netty4] class Netty4ClientChannelInitializer[In, Out](
    params: Stack.Params,
    encoder: Option[FrameEncoder[In]] = None,
    decoderFactory: Option[() => FrameDecoder[Out]] = None)
  extends AbstractNetty4ClientChannelInitializer[In, Out](params) {
  import Netty4ClientChannelInitializer._

  private[this] val encodeHandler = encoder.map(new EncodeHandler[In](_))
  private[this] val decodeHandler = decoderFactory.map(new DecodeHandler[Out](_))

  override def initChannel(ch: Channel): Unit = {
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
    params: Stack.Params)
  extends ChannelInitializer[Channel] {
    import Netty4ClientChannelInitializer._

    private[this] val Transport.Liveness(readTimeout, writeTimeout, _) = params[Transport.Liveness]
    private[this] val Logger(logger) = params[Logger]
    private[this] val Stats(stats) = params[Stats]

    private[this] val exceptionHandler = new ChannelExceptionHandler(stats, logger)

    def initChannel(ch: Channel): Unit = {
      val pipe = ch.pipeline

      if (readTimeout.isFinite) {
        val (timeoutValue, timeoutUnit) = readTimeout.inTimeUnit
        pipe.addFirst(ReadTimeoutHandlerKey, new ReadTimeoutHandler(timeoutValue, timeoutUnit))
      }

      if (writeTimeout.isFinite) {
        val (timeoutValue, timeoutUnit) = writeTimeout.inTimeUnit
        pipe.addLast(WriteTimeoutHandlerKey, new WriteTimeoutHandler(timeoutValue, timeoutUnit))
      }

      pipe.addLast("exception handler", exceptionHandler)
    }
  }

/**
 * Channel Initializer which exposes the netty pipeline to the transporter.
 * @param params configuration parameters.
 * @param pipeCb a callback for initialized pipelines
 * @tparam In the application request type.
 * @tparam Out the application response type.
 */
private[netty4] class RawNetty4ClientChannelInitializer[In, Out](
    params: Stack.Params,
    pipeCb: ChannelPipeline => Unit)
  extends AbstractNetty4ClientChannelInitializer[In, Out](params) {

  override def initChannel(ch: Channel): Unit = {
    super.initChannel(ch)
    pipeCb(ch.pipeline)
  }
}
