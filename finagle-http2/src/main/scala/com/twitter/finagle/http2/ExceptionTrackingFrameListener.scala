package com.twitter.finagle.http2

import com.twitter.app.GlobalFlag
import com.twitter.finagle.stats.StatsReceiver
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http2.Http2Exception.StreamException
import io.netty.handler.codec.http2.{
  Http2Exception,
  Http2Flags,
  Http2FrameListener,
  Http2FrameListenerDecorator,
  Http2Headers,
  Http2Settings
}

object trackH2SessionExceptions
    extends GlobalFlag[Boolean](
      default = false,
      help = "Track H2 session exceptions"
    )

/**
 * Frame listener that counts up the different types of exceptions that are thrown by
 * the parent `FrameListener`.
 */
private class ExceptionTrackingFrameListener(
  statsReceiver: StatsReceiver,
  underlying: Http2FrameListener)
    extends Http2FrameListenerDecorator(underlying) {

  private[this] def http2Exception(ex: Http2Exception): Nothing = {
    val scope = ex match {
      case _: StreamException => "stream_exceptions"
      case _ => "connection_exceptions"
    }

    statsReceiver.counter(scope, ex.error.name).incr()
    // finally rethrow since the codec needs to get this.
    throw ex
  }

  override def onHeadersRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    headers: Http2Headers,
    padding: Int,
    endStream: Boolean
  ): Unit = {
    try super.onHeadersRead(ctx, streamId, headers, padding, endStream)
    catch { case e: Http2Exception => http2Exception(e) }
  }

  override def onHeadersRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    headers: Http2Headers,
    streamDependency: Int,
    weight: Short,
    exclusive: Boolean,
    padding: Int,
    endStream: Boolean
  ): Unit = {
    try super.onHeadersRead(
      ctx,
      streamId,
      headers,
      streamDependency,
      weight,
      exclusive,
      padding,
      endStream
    )
    catch { case e: Http2Exception => http2Exception(e) }
  }

  override def onPushPromiseRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    promisedStreamId: Int,
    headers: Http2Headers,
    padding: Int
  ): Unit = {
    try super.onPushPromiseRead(ctx, streamId, promisedStreamId, headers, padding)
    catch { case e: Http2Exception => http2Exception(e) }
  }

  override def onDataRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    data: ByteBuf,
    padding: Int,
    endOfStream: Boolean
  ): Int =
    try super.onDataRead(ctx, streamId, data, padding, endOfStream)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onPriorityRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    streamDependency: Int,
    weight: Short,
    exclusive: Boolean
  ): Unit =
    try super.onPriorityRead(ctx, streamId, streamDependency, weight, exclusive)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onRstStreamRead(ctx: ChannelHandlerContext, streamId: Int, errorCode: Long): Unit =
    try super.onRstStreamRead(ctx, streamId, errorCode)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onSettingsAckRead(ctx: ChannelHandlerContext): Unit =
    try super.onSettingsAckRead(ctx)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onSettingsRead(ctx: ChannelHandlerContext, settings: Http2Settings): Unit =
    try super.onSettingsRead(ctx, settings)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onPingRead(ctx: ChannelHandlerContext, data: Long): Unit =
    try super.onPingRead(ctx, data)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onPingAckRead(ctx: ChannelHandlerContext, data: Long): Unit =
    try super.onPingAckRead(ctx, data)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onGoAwayRead(
    ctx: ChannelHandlerContext,
    lastStreamId: Int,
    errorCode: Long,
    debugData: ByteBuf
  ): Unit =
    try super.onGoAwayRead(ctx, lastStreamId, errorCode, debugData)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onWindowUpdateRead(
    ctx: ChannelHandlerContext,
    streamId: Int,
    windowSizeIncrement: Int
  ): Unit =
    try super.onWindowUpdateRead(ctx, streamId, windowSizeIncrement)
    catch { case e: Http2Exception => http2Exception(e) }

  override def onUnknownFrame(
    ctx: ChannelHandlerContext,
    frameType: Byte,
    streamId: Int,
    flags: Http2Flags,
    payload: ByteBuf
  ): Unit =
    try super.onUnknownFrame(ctx, frameType, streamId, flags, payload)
    catch { case e: Http2Exception => http2Exception(e) }
}
