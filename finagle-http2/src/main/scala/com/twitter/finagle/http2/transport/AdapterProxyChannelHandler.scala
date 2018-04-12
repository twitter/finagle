package com.twitter.finagle.http2.transport

import com.twitter.finagle.FailureFlags
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.logging.{Logger, Level, HasLogLevel}
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
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
  private[this] var cur = -1

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

  // this may return a null.
  private[this] def getEmbeddedChannel(
    ctx: ChannelHandlerContext,
    streamId: Int
  ): EmbeddedChannel =
    map.get(streamId) match {
      case Some(completingChannel) =>
        completingChannel.embedded
      case None if (streamId > cur) =>
        cur = streamId
        val completingChannel = new CompletingChannel(setupEmbeddedChannel(ctx, streamId))
        map.put(streamId, completingChannel)
        completingChannel.embedded
      case None =>
        null
    }

  private[this] def closeAll(): Unit = {
    map.foreach {
      case (_, value) =>
        // Clean up inbound handlers. Normally the close
        // below would be sufficient but it is suppressed
        // by the OutboundPropagator so we have to take
        // matters into our own hands and fire the channel
        // inactive explicitly.
        value.embedded.pipeline().fireChannelInactive()
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

  private[this] def updateCompletionStatus(obj: HttpObject, streamId: Int, reading: Boolean): Unit = obj match {
    case _: LastHttpContent =>
      val completed = map(streamId)
      if (reading) {
        completed.finishedReading = true
      } else {
        completed.finishedWriting = true
      }
      if (completed.finishedWriting && completed.finishedReading)
        rmStream(streamId)
    case _ => // nop
  }

  // stop tracking a stream and allow the associated embedded handlers to cleanup.
  private def rmStream(streamId: Int): Unit = map.remove(streamId) match {
    case Some(channel) =>
      channel.embedded.pipeline().fireChannelInactive()
    case None =>
      if (log.isLoggable(Level.DEBUG)) log.debug(s"tried to remove non-existent stream #$streamId")
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case Message(obj, streamId) =>
      getEmbeddedChannel(ctx, streamId) match {
        case null =>
          // We've sent an RST, and won't be doing anything more to this stream id, so just free the message
          ReferenceCountUtil.release(obj)
        case embedded =>
          embedded.writeInbound(obj)
          updateCompletionStatus(obj, streamId, true)
      }
    case rst @ Rst(streamId, _) =>
      rmStream(streamId)
      ctx.fireChannelRead(rst)
    case goaway: GoAway => ctx.fireChannelRead(goaway)
    case Ping => ctx.fireChannelRead(Ping)
    case exn: StreamException => ctx.fireChannelRead(exn)
    case upgrade: UpgradeRequestHandler.UpgradeResult => ctx.fireChannelRead(upgrade)
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
            getEmbeddedChannel(ctx, streamId) match {
              case null =>
                promise.setFailure(new WriteToNackedStreamException(streamId))
                // nop, we've received an rst, and won't be doing anything more to this stream id
              case embedded =>
                // NB: We flush the channel here to avoid iterating through all EmbeddedChannels on each
                // call to flush()
                embedded.writeAndFlush(obj, proxyForChannel(embedded, promise))
                updateCompletionStatus(obj, streamId, false)
            }
          case rst @ Rst(streamId, _) =>
            rmStream(streamId)
            // receiving an rst for a stream that never existed is a protocol error, but
            // we handle that in StreamTransportFactory, so for now we just propagate it.
            ctx.write(rst, promise)
          case goaway: GoAway => ctx.write(goaway, promise)
          case Ping => ctx.write(Ping, promise)
          case StreamException(exn, _) =>
            // this doesn't make sense.  fail hard here.
            val wrongType = new IllegalArgumentException(
              "StreamExceptions can not be written to the netty pipeline.",
              exn
            )
            log.error(wrongType, "Tried to write a stream exception to the http2 client pipeline")
            promise.setFailure(wrongType)
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

  // this is non-retryable because we know this message is mid-stream, so we've
  // already written stuff to the wire.
  class WriteToNackedStreamException(
    id: Int,
    private[finagle] val flags: Long = FailureFlags.NonRetryable
  ) extends Exception(s"Tried to write to already nacked stream id $id.")
      with FailureFlags[WriteToNackedStreamException]
      with HasLogLevel {
    def logLevel: Level = Level.DEBUG
    protected def copyWithFlags(flags: Long): WriteToNackedStreamException =
      new WriteToNackedStreamException(id, flags)
  }
}
