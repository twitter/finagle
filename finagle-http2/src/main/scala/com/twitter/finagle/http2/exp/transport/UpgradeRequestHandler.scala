package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.http2.MultiplexCodecBuilder
import com.twitter.finagle.http2.transport.{H2Filter, H2StreamChannelInit, Http2UpgradingTransport}
import com.twitter.finagle.netty4.Netty4Listener.BackPressure
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.{Stats, Timer}
import com.twitter.finagle.Stack
import io.netty.channel._
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.{FullHttpRequest, HttpClientCodec, HttpClientUpgradeHandler}
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * Takes the upgrade result and marks it as something read off the wire to
 * expose it to finagle, and manipulates the pipeline to be fit for http/2.
 */
private final class UpgradeRequestHandler(params: Stack.Params, httpClientCodec: HttpClientCodec)
    extends ChannelDuplexHandler {

  import UpgradeRequestHandler._

  private[this] val statsReceiver = params[Stats].statsReceiver.scope("upgrade")
  private[this] val attemptCounter = statsReceiver.counter("attempt")
  private[this] val upgradeCounter = statsReceiver.counter("success")
  private[this] val ignoredCounter = statsReceiver.counter("ignored")

  // Exposed for testing
  def initializeUpgradeStreamChannel(ch: Channel, parentCtx: ChannelHandlerContext): Unit = {
    val p = parentCtx.pipeline
    p.asScala.toList
      .dropWhile(_.getKey != HandlerName)
      .tail
      .takeWhile(_.getKey != ChannelTransport.HandlerName)
      .foreach { entry =>
        p.remove(entry.getValue)
      }

    p.addBefore(HandlerName, H2Filter.HandlerName, new H2Filter(params[Timer].timer))
    val streamChannelInit = H2StreamChannelInit.initClient(params)
    val clientSession = new ClientSessionImpl(params, streamChannelInit, parentCtx.channel)

    upgradeCounter.incr()
    // let the Http2UpgradingTransport know that this was an upgrade request
    parentCtx.pipeline.remove(this)

    ch.pipeline.addLast(streamChannelInit)

    val trans = clientSession.newChildTransport(ch)
    parentCtx.fireChannelRead(
      Http2UpgradingTransport.UpgradeSuccessful(_ =>
        new DeferredCloseSession(clientSession, trans.onClose.unit) -> new SingleDispatchTransport(
          trans))
    )
  }

  private[this] def addUpgradeHandler(ctx: ChannelHandlerContext): Unit = {

    val upgradeStreamhandler: ChannelHandler = new ChannelInitializer[Channel] {
      def initChannel(ch: Channel): Unit = initializeUpgradeStreamChannel(ch, ctx)
    }

    val multiplex = MultiplexCodecBuilder.clientMultiplexCodec(params, Some(upgradeStreamhandler))

    val upgradeCodec = new Http2ClientUpgradeCodec(multiplex)
    // The parameter for `HttpClientUpgradeHandler.maxContentLength` can be 0 because
    // the HTTP2 spec requires that a 101 request not have a body and for any other
    // response status it will remove itself from the pipeline.
    val upgradeHandler = new HttpClientUpgradeHandler(httpClientCodec, upgradeCodec, 0)
    ctx.pipeline.addBefore(ctx.name, "httpUpgradeHandler", upgradeHandler)
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    msg match {
      case req: FullHttpRequest if req.content.readableBytes == 0 =>
        // A request we can upgrade from. Reshape our pipeline and keep trucking.
        addUpgradeHandler(ctx)
        attemptCounter.incr()
        super.write(ctx, msg, promise)

      case _ =>
        // we don't attempt to upgrade when the request may have content, so we remove
        // ourselves and let the backend handlers know that we're not going to try upgrading.
        ignoredCounter.incr()
        ctx.pipeline.remove(this)

        // Configure the original backpressure strategy since the pipeline started life
        // with autoread enabled.
        ctx.channel.config.setAutoRead(!params[BackPressure].enabled)
        ctx.fireUserEventTriggered(Http2UpgradingTransport.UpgradeAborted)
        ctx.write(msg, promise)
    }
  }

  override def userEventTriggered(ctx: ChannelHandlerContext, event: Any): Unit = event match {
    case UpgradeEvent.UPGRADE_ISSUED => // no surprises here.

    case UpgradeEvent.UPGRADE_REJECTED =>
      // Configure the original backpressure strategy since the pipeline started life
      // with autoread enabled.
      ctx.channel.config.setAutoRead(!params[BackPressure].enabled)
      ctx.pipeline.remove(this)
      ctx.fireChannelRead(Http2UpgradingTransport.UpgradeRejected)

    case _ =>
      super.userEventTriggered(ctx, event)
  }
}

private object UpgradeRequestHandler {
  val HandlerName = "pipelineUpgrader"
}
