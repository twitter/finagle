package com.twitter.finagle.http2.transport

import com.twitter.finagle.netty4.http.exp.initClient
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.param.Stats
import com.twitter.finagle.{Stack, FailureFlags}
import com.twitter.util.Promise
import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise, ChannelFutureListener, ChannelFuture}
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.LastHttpContent
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * Takes the upgrade result and marks it as something read off the wire to
 * expose it to finagle, and manipulates the pipeline to be fit for http/2.
 */
private[http2] class UpgradeRequestHandler(
    params: Stack.Params)
  extends ChannelDuplexHandler {

  import UpgradeRequestHandler._

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")
  private[this] val writeFinished = Promise[Unit]()

  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    if (msg.isInstanceOf[LastHttpContent]) {
      promise.addListener(new ChannelFutureListener {
        def operationComplete(f: ChannelFuture): Unit = {
          if (f.isSuccess) {
            writeFinished.setDone()
          } else if (f.isCancelled()) {
            writeFinished.setException(new CancelledUpgradeException)
          } else {
            writeFinished.setException(f.cause)
          }
        }
      })
    }
    super.write(ctx, msg, promise)
  }

  override def userEventTriggered(ctx: ChannelHandlerContext, event: Any): Unit = {
    event match {
      case rejected@UpgradeEvent.UPGRADE_REJECTED =>
        ctx.fireChannelRead(rejected)
        // disable autoread if we fail the upgrade
        ctx.channel.config.setAutoRead(false)
        ctx.pipeline.remove(this)
      case successful@UpgradeEvent.UPGRADE_SUCCESSFUL =>
        writeFinished.onSuccess { _ =>
          val p = ctx.pipeline
          p.asScala
            .toList
            .dropWhile(_.getKey != HandlerName)
            .tail
            .takeWhile(_.getKey != ChannelTransport.HandlerName)
            .foreach { entry => p.remove(entry.getValue) }
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
        }
      case _ => // nop
    }
    super.userEventTriggered(ctx, event)
  }
}

private[http2] object UpgradeRequestHandler {
  val HandlerName = "pipelineUpgrader"

  class CancelledUpgradeException(private[finagle] val flags: Long = FailureFlags.Empty)
    extends Exception("the last write of an upgrade request was cancelled")
    with FailureFlags[CancelledUpgradeException] {

    protected def copyWithFlags(newFlags: Long): CancelledUpgradeException =
      new CancelledUpgradeException(newFlags)
  }
}
