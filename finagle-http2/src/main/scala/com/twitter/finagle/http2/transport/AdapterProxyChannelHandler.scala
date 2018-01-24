package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.logging.Logger
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.{HttpObject, LastHttpContent}
import io.netty.util.ReferenceCountUtil
import io.netty.util.concurrent.PromiseCombiner
import scala.collection.mutable.HashMap

/**
 * AdapterProxyChannelHandler processes [[Message]]s like HttpObjects, but
 * preserves the stream information.
 *
 * AdapterProxyChannelHandler models each stream as its own channel, to
 * disambiguate which stream an HttpObject belongs to, as well as to allow
 * aggregating or disaggregating handlers to work properly.
 *
 * It is not Sharable.
 *
 * @param setupFn is run on each of the channels for each of the streams, and
 *                should be for a pipeline for HttpObject.
 */
private[http2] final class AdapterProxyChannelHandler(
  setupFn: ChannelPipeline => Unit,
  statsReceiver: StatsReceiver = NullStatsReceiver
) extends ChannelDuplexHandler {

  import AdapterProxyChannelHandler._

  // this handler cannot be shared, and depends on netty's thread model to avoid
  // having to synchronize
  private[this] val map = HashMap[Int, CompletingChannel]()
  private[this] val log = Logger.get()

  private[this] val channelSizeGauge = statsReceiver.addGauge("channels") {
    synchronized { map.size }
  }

  private[this] def setupEmbeddedChannel(
    ctx: ChannelHandlerContext,
    streamId: Int
  ): EmbeddedChannel = {
    val embedded = new EmbeddedChannel(ctx.channel.id)
    val p = embedded.pipeline
    setupFn(p)
    p.addFirst(new OutboundPropagator(ctx, streamId))
    p.addLast(new InboundPropagator(ctx, streamId))
    embedded
  }

  private[this] def getEmbeddedChannel(
    ctx: ChannelHandlerContext,
    streamId: Int
  ): EmbeddedChannel =
    map
      .getOrElseUpdate(
        streamId,
        new CompletingChannel(setupEmbeddedChannel(ctx, streamId))
      )
      .embedded

  private[this] def closeAll(): Unit = {
    map.foreach {
      case (_, value) =>
        value.embedded.close()
    }
    map.clear()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    closeAll()
    // although we typically propagate these methods through the embedded
    // channel, we call closeAll here instead of channelInactive, so it's
    // impossible to propagate properly.  instead, we manually propagate it
    // ourselves.
    super.channelInactive(ctx)
  }

  override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
    closeAll()
  }

  override def close(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
    val combiner = new PromiseCombiner()
    map.foreach {
      case (_, value) =>
        val p = value.embedded.newPromise()
        combiner.add(p)
        value.embedded.close(p)
    }
    val streamsClosed = ctx.newPromise()
    combiner.finish(streamsClosed)
    streamsClosed.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture): Unit = ctx.close(promise)
    })
  }

  // NB: Iterating through all EmbeddedChannels takes a while. Since we always
  // call flush() after write(...) anyway, we manually flush the EmbeddedChannels
  // after they're written to. For non-embedded write (RST/GOAWAY/PING), the default
  // flush() behavior is sufficient

  private[this] def updateCompletionStatus(obj: HttpObject, streamId: Int, reading: Boolean): Unit =
    obj match {
      case _: LastHttpContent =>
        val completed = map(streamId)
        if (reading) {
          completed.finishedReading = true
        } else {
          completed.finishedWriting = true
        }
        if (completed.finishedWriting && completed.finishedReading)
          map.remove(streamId)
      case _ => // nop
    }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case Message(obj, streamId) =>
      getEmbeddedChannel(ctx, streamId).writeInbound(obj)
      updateCompletionStatus(obj, streamId, true)
    case rst: Rst => ctx.fireChannelRead(rst)
    case goaway: GoAway => ctx.fireChannelRead(goaway)
    case Ping => ctx.fireChannelRead(Ping)
    case upgrade: UpgradeEvent => ctx.fireChannelRead(upgrade)
    case _ =>
      val wrongType = new IllegalArgumentException(
        s"Expected a StreamMessage or UpgradeEvent, got ${msg.getClass.getName} instead."
      )
      ReferenceCountUtil.release(msg)
      log.error(wrongType, "Tried to write the wrong type to the http2 client pipeline")
      ctx.fireExceptionCaught(wrongType)
  }

  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    msg match {
      case strm: StreamMessage =>
        strm match {
          case Message(obj, streamId) =>
            val embedded = getEmbeddedChannel(ctx, streamId)
            // NB: We flush the channel here to avoid iterating through all EmbeddedChannels on each
            // call to flush()
            embedded.writeAndFlush(obj, proxyForChannel(embedded, promise))
            updateCompletionStatus(obj, streamId, false)
          case rst: Rst => ctx.write(rst, promise)
          case goaway: GoAway => ctx.write(goaway, promise)
          case Ping => ctx.write(Ping, promise)
        }
      case _ =>
        val wrongType = new IllegalArgumentException(
          s"Expected a StreamMessage, got ${msg.getClass.getName} instead."
        )
        ReferenceCountUtil.release(msg)
        log.error(wrongType, "Tried to write the wrong type to the http2 client pipeline")
        promise.setFailure(wrongType)
    }
  }

  /**
   * CompletingChannel helps to keep track of the state of a given stream.
   */
  class CompletingChannel(val embedded: EmbeddedChannel) {
    var finishedWriting = false
    var finishedReading = false
  }

  /**
   * Propagates read messages from the inner per-stream pipeline back out to the
   * normal ChannelPipeline.
   *
   * Using this model instead of calling `readInboundMessage` allows
   * asynchronous handlers to work properly, as well as handlers that aggregate
   * or disaggregate message.
   */
  class InboundPropagator(underlying: ChannelHandlerContext, streamId: Int)
      extends ChannelInboundHandlerAdapter {

    override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
      underlying.fireChannelRead(Message(msg.asInstanceOf[HttpObject], streamId))
    }
    override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
      underlying.fireChannelReadComplete()
    }
    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      underlying.fireExceptionCaught(cause)
    }
  }

  /**
   * Propagates written messages from the inner per-stream pipeline back out to
   * the normal ChannelPipeline.
   *
   * Using this model instead of calling `readOutboundMessage` allows
   * asynchronous handlers to work properly, as well as handlers that aggregate
   * or disaggregate message.
   */
  class OutboundPropagator(underlying: ChannelHandlerContext, streamId: Int)
      extends ChannelOutboundHandlerAdapter {

    import AdapterProxyChannelHandler._

    override def flush(ctx: ChannelHandlerContext): Unit = {
      underlying.flush()
    }

    override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
      underlying.write(
        Message(msg.asInstanceOf[HttpObject], streamId),
        proxyForChannel(underlying.channel, promise)
      )
    }

    override def close(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
      underlying.close(proxyForChannel(underlying.channel, promise))
    }
  }

  private[http2] def numConnections: Int = map.size
}

private[http2] object AdapterProxyChannelHandler {
  val HandlerName = "aggregate"

  /**
   * Creates a new promise that can be used instead of the `sink` promise, but
   * has the right Channel.
   */
  def proxyForChannel(channel: Channel, sink: ChannelPromise): ChannelPromise = {
    val source = channel.newPromise()
    source.addListener(new ChannelPromiseNotifier(sink))
    source
  }
}
