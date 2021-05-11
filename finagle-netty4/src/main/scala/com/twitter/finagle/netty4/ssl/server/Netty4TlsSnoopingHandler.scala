package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.finagle.param.Stats
import com.twitter.finagle.ssl.TlsSnooping
import com.twitter.finagle.ssl.TlsSnooping.DetectionResult
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import java.util

/**
 * Handler that will drive TLS snooping.
 *
 * This handler will accumulate data to feed to the configured snooper function. Once
 * it receives a signal from the snooper it will install the TLS handler (if necessary)
 * and signal to the downstream handlers what the result was using a user event of type
 * `Netty4TlsSnoopingHandler.Result`, then remove itself forwarding the accumulated
 * bytes to subsequent handlers.
 */
private[finagle] final class Netty4TlsSnoopingHandler(params: Params) extends ByteToMessageDecoder {
  import Netty4TlsSnoopingHandler._

  // This handler will intercept channel-active events and defer them until after
  // snooping is complete. This is useful when later handlers expect to know whether
  // or not the pipeline is TLS when they themselves become active. However, if we
  // add this handler to an already active pipeline we don't want to unduly fire the
  // event, so we use this state to keep track of whether we intercepted one or not.
  private[this] var channelActiveDeferred = false

  private[this] val snoopingCounter = params[Stats].statsReceiver.counter("tls", "snooped_connects")
  private[this] val snooper = params[TlsSnooping.Param].snooper

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    if (ctx.channel.isActive) {
      readIfNecessary(ctx)
    }
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    channelActiveDeferred = true
    readIfNecessary(ctx)
  }

  def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val finished = snooper(ByteBufConversion.copyByteBufToBuf(in)) match {
      case DetectionResult.Cleartext =>
        cleartextDetected(ctx)
        true

      case DetectionResult.Secure =>
        secureDetected(ctx)
        true

      case DetectionResult.NeedMoreData =>
        readIfNecessary(ctx)
        false
    }

    if (finished) {
      maybeFireChannelActive(ctx)
      ctx.pipeline.remove(this)
    }
  }

  private[this] def maybeFireChannelActive(ctx: ChannelHandlerContext): Unit = {
    if (channelActiveDeferred) {
      channelActiveDeferred = false
      ctx.fireChannelActive()
    }
  }

  private[this] def readIfNecessary(ctx: ChannelHandlerContext): Unit = {
    if (!ctx.channel.config.isAutoRead) {
      ctx.read()
    }
  }

  private[this] def cleartextDetected(ctx: ChannelHandlerContext): Unit =
    ctx.fireUserEventTriggered(Result.Cleartext)

  private[this] def secureDetected(ctx: ChannelHandlerContext): Unit = {
    snoopingCounter.incr()
    ctx.pipeline.addAfter(
      ctx.name,
      Netty4ServerSslChannelInitializer.HandlerName,
      new Netty4ServerSslChannelInitializer(params))
    ctx.fireUserEventTriggered(Result.Secure)
  }
}

private[finagle] object Netty4TlsSnoopingHandler {

  /** Name that should be used when inserting this handler into a Netty pipeline */
  val HandlerName: String = "tlsSnooper"

  /** Signal event for downstream handlers */
  sealed abstract class Result private ()
  object Result {

    /** Encryption was not detected and the SslHandler was not installed */
    case object Cleartext extends Result

    /** Encryption was detected and the SslHandler was installed */
    case object Secure extends Result
  }
}
