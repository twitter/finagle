package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.transport.Http2UpgradingTransport.{
  UpgradeAborted,
  UpgradeRejected,
  UpgradeSuccessful
}
import com.twitter.finagle.netty4.Netty4Listener.BackPressure
import com.twitter.finagle.netty4.http.initClient
import com.twitter.finagle.netty4.transport.{ChannelTransport, HasExecutor}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.{FailureFlags, Stack}
import io.netty.channel._
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.{FullHttpRequest, HttpClientCodec, HttpClientUpgradeHandler}
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * Takes the upgrade result and marks it as something read off the wire to
 * expose it to finagle, and manipulates the pipeline to be fit for http/2.
 */
private[http2] final class UpgradeRequestHandler(
  params: Stack.Params,
  httpClientCodec: HttpClientCodec,
  connectionHandlerBuilder: RichHttpToHttp2ConnectionHandlerBuilder)
    extends ChannelDuplexHandler {

  import UpgradeRequestHandler._

  private[this] val statsReceiver = params[Stats].statsReceiver.scope("upgrade")
  private[this] val attemptCounter = statsReceiver.counter("attempt")
  private[this] val upgradeCounter = statsReceiver.counter("success")
  private[this] val ignoredCounter = statsReceiver.counter("ignored")

  // Used by the `Http2UpgradingTransport` to build a H2 session out of the `ChannelTransport`
  private[this] def upgradeMessage = UpgradeSuccessful { underlying =>
    val inOutCasted = Transport.cast[StreamMessage, StreamMessage](underlying)
    val contextCasted = inOutCasted.asInstanceOf[
      Transport[StreamMessage, StreamMessage] {
        type Context = TransportContext with HasExecutor
      }
    ]
    val fac = new StreamTransportFactory(contextCasted, underlying.remoteAddress, params)
    fac -> fac.first()
  }

  private[this] def addUpgradeHandler(ctx: ChannelHandlerContext): Unit = {
    // Reshape the pipeline
    val connectionHandler = connectionHandlerBuilder.build()
    val upgradeCodec = new Http2ClientUpgradeCodec(connectionHandler)
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
        ctx.fireUserEventTriggered(UpgradeAborted)
        ctx.write(msg, promise)
    }
  }

  override def userEventTriggered(ctx: ChannelHandlerContext, event: Any): Unit = {
    event match {
      case UpgradeEvent.UPGRADE_ISSUED => // no surprises here.

      case UpgradeEvent.UPGRADE_REJECTED =>
        // Configure the original backpressure strategy since the pipeline started life
        // with autoread enabled.
        ctx.channel.config.setAutoRead(!params[BackPressure].enabled)
        ctx.pipeline.remove(this)
        ctx.fireUserEventTriggered(UpgradeRejected)

      case UpgradeEvent.UPGRADE_SUCCESSFUL =>
        prepareForUpgrade(ctx)

      case _ =>
        super.userEventTriggered(ctx, event)
    }
  }

  private[this] def prepareForUpgrade(ctx: ChannelHandlerContext): Unit = {
    // This removes us from the transport pathway. We do the pipeline modifications
    // in this method so that we know we're in the right executor.
    val p = ctx.pipeline
    p.asScala.toList
      .dropWhile(_.getKey != HandlerName)
      .tail
      .takeWhile(_.getKey != ChannelTransport.HandlerName)
      .foreach { entry =>
        p.remove(entry.getValue)
      }
    p.addBefore(
      ChannelTransport.HandlerName,
      AdapterProxyChannelHandler.HandlerName,
      new AdapterProxyChannelHandler(
        { pipeline: ChannelPipeline =>
          pipeline.addLast(SchemifyingHandler.HandlerName, new SchemifyingHandler("http"))
          pipeline.addLast(StripHeadersHandler.HandlerName, StripHeadersHandler)
          initClient(params)(pipeline)
        },
        statsReceiver.scope("adapter_proxy")
      )
    )
    upgradeCounter.incr()
    // let the Http2UpgradingTransport know that this was an upgrade request
    ctx.pipeline.remove(this)

    ctx.fireChannelRead(upgradeMessage)
  }
}

private[http2] object UpgradeRequestHandler {
  val HandlerName = "pipelineUpgrader"

  class CancelledUpgradeException(val flags: Long = FailureFlags.Empty)
      extends Exception("the last write of an upgrade request was cancelled")
      with FailureFlags[CancelledUpgradeException] {

    protected def copyWithFlags(newFlags: Long): CancelledUpgradeException =
      new CancelledUpgradeException(newFlags)
  }
}
