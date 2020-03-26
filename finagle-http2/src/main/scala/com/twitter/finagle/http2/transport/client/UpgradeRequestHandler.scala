package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.MultiplexHandlerBuilder
import com.twitter.finagle.http2.transport.client.H2Pool.OnH2Service
import com.twitter.finagle.http2.transport.common.H2StreamChannelInit
import com.twitter.finagle.netty4.Netty4Listener.BackPressure
import com.twitter.finagle.netty4.http.{Http2CodecName, Http2MultiplexHandlerName}
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.Transport
import io.netty.channel._
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.{
  FullHttpRequest,
  FullHttpResponse,
  HttpClientCodec,
  HttpClientUpgradeHandler
}
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec
import scala.jdk.CollectionConverters._

/**
 * Takes the upgrade result and marks it as something read off the wire to
 * expose it to finagle, and manipulates the pipeline to be fit for http/2.
 */
private final class UpgradeRequestHandler(
  params: Stack.Params,
  onH2Service: OnH2Service,
  httpClientCodec: HttpClientCodec,
  modifier: Transport[Any, Any] => Transport[Any, Any])
    extends ChannelDuplexHandler {

  import UpgradeRequestHandler._

  private[this] val stats = params[Stats].statsReceiver
  private[this] val statsReceiver = stats.scope("upgrade")
  private[this] val attemptCounter = statsReceiver.counter("attempt")
  private[this] val upgradeCounter = statsReceiver.counter("success")
  private[this] val ignoredCounter = statsReceiver.counter("ignored")

  // Exposed for testing
  def initializeUpgradeStreamChannel(ch: Channel, parentCtx: ChannelHandlerContext): Unit = {
    val p = parentCtx.pipeline
    cleanPipeline(p)

    val pingDetectionHandler = new H2ClientFilter(params)
    p.addBefore(HandlerName, H2ClientFilter.HandlerName, pingDetectionHandler)
    val streamChannelInit = H2StreamChannelInit.initClient(params)
    val clientSession = new ClientSessionImpl(
      params,
      streamChannelInit,
      parentCtx.channel,
      () => pingDetectionHandler.status)

    upgradeCounter.incr()
    // let the Http2UpgradingTransport know that this was an upgrade request
    parentCtx.pipeline.remove(this)

    ch.pipeline.addLast(streamChannelInit)

    val trans = clientSession.newChildTransport(ch)

    // We need to make sure that if we close the session, it doesn't
    // close everything down until the first stream has finished.
    val session = new DeferredCloseSession(clientSession, trans.onClose.unit)
    onH2Service(new ClientServiceImpl(session, stats, modifier))

    parentCtx.fireChannelRead(
      Http2UpgradingTransport.UpgradeSuccessful(new SingleDispatchTransport(trans))
    )
  }

  private[this] def addUpgradeHandler(ctx: ChannelHandlerContext): Unit = {

    val upgradeStreamhandler: ChannelHandler = new ChannelInitializer[Channel] {
      def initChannel(ch: Channel): Unit = initializeUpgradeStreamChannel(ch, ctx)
    }

    val (codec, handler) =
      MultiplexHandlerBuilder.clientFrameCodec(params, Some(upgradeStreamhandler))

    val upgradeCodec = new Http2ClientUpgradeCodec(codec) {
      override def upgradeTo(
        ctx: ChannelHandlerContext,
        upgradeResponse: FullHttpResponse
      ): Unit = {
        // Add the handler to the pipeline.
        ctx.pipeline
          .addAfter(ctx.name, Http2CodecName, codec)
          .addAfter(Http2CodecName, Http2MultiplexHandlerName, handler)

        // Reserve local stream for the response with stream id of '1'
        codec.onHttpClientUpgrade()
      }
    }
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
        noUpgrade(ctx, Http2UpgradingTransport.UpgradeAborted)
        ctx.write(msg, promise)
    }
  }

  override def userEventTriggered(ctx: ChannelHandlerContext, event: Any): Unit = event match {
    case UpgradeEvent.UPGRADE_ISSUED => // no surprises here.

    case UpgradeEvent.UPGRADE_REJECTED =>
      noUpgrade(ctx, Http2UpgradingTransport.UpgradeRejected)

    case _ =>
      super.userEventTriggered(ctx, event)
  }

  private[this] def noUpgrade(
    ctx: ChannelHandlerContext,
    result: Http2UpgradingTransport.UpgradeResult
  ): Unit = {
    ctx.pipeline.remove(this)
    ctx.fireChannelRead(result)

    // Configure the original backpressure strategy since the pipeline started life
    // with autoread enabled.
    ctx.channel.config.setAutoRead(!params[BackPressure].enabled)
    // Make sure we request at least one more message so that we don't starve the
    // ChannelTransport.
    ctx.read()
  }
}

private object UpgradeRequestHandler {
  val HandlerName = "pipelineUpgrader"

  // Clean out the channel handlers that are only for HTTP/1.x so we don't have
  // a bunch of noise in our main pipeline.
  private def cleanPipeline(pipeline: ChannelPipeline): Unit = {
    pipeline.asScala.toList
    // We don't want to remove anything up to the pipeline upgrader which
    // are stages like metrics, etc.
      .dropWhile(_.getKey != HandlerName)
      .drop(1)
      // These will be things that operate on HTTP messages which will no longer
      // be flowing down the main pipeline. Examples include message aggregators,
      // compressors/decompressors, etc.
      .takeWhile(_.getKey != ChannelTransport.HandlerName)
      .foreach { entry => pipeline.remove(entry.getValue) }
  }
}
