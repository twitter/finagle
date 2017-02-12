package com.twitter.finagle.http2.transport

import com.twitter.finagle.netty4.http.exp.initClient
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.Stats
import com.twitter.finagle.Stack
import io.netty.channel.{ChannelInboundHandlerAdapter, ChannelHandlerContext}
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * Takes the upgrade result and marks it as something read off the wire to
 * expose it to finagle, and manipulates the pipeline to be fit for http/2.
 */
private[http2] class UpgradeRequestHandler(
    params: Stack.Params)
  extends ChannelInboundHandlerAdapter {

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")

  override def userEventTriggered(ctx: ChannelHandlerContext, event: Any): Unit = {
    event match {
      case rejected@UpgradeEvent.UPGRADE_REJECTED =>
        ctx.fireChannelRead(rejected)
        // disable autoread if we fail the upgrade
        ctx.channel.config.setAutoRead(false)
        ctx.pipeline.remove(this)
      case successful@UpgradeEvent.UPGRADE_SUCCESSFUL =>
        val p = ctx.pipeline
        p.asScala
          .toList
          .dropWhile(_.getKey != UpgradeRequestHandler.HandlerName)
          .tail
          .takeWhile(_.getKey != ChannelTransport.HandlerName)
          .foreach { entry =>
            p.remove(entry.getValue)
          }
        p.addBefore(
          ChannelTransport.HandlerName,
          "aggregate",
          new AdapterProxyChannelHandler({ pipeline =>
            pipeline.addLast("schemifier", new SchemifyingHandler("http"))
            initClient(params)(pipeline)
          })
        )
        upgradeCounter.incr()
        ctx.fireChannelRead(successful)
        ctx.pipeline.remove(this)
      case _ => // nop
    }
    super.userEventTriggered(ctx, event)
  }
}

private[http2] object UpgradeRequestHandler {
  val HandlerName = "pipelineUpgrader"
}
