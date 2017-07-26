package com.twitter.finagle.netty4.http.handler

import com.twitter.finagle.http.Fields
import com.twitter.util.StorageUnit
import io.netty.buffer.ByteBufHolder
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http._
import java.util.logging.{Level, Logger}

private object PayloadSizeHandler {
  def mk413(v: HttpVersion): FullHttpResponse = {
    val resp = new DefaultFullHttpResponse(v, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)
    resp.headers().set(Fields.Connection, "close")
    resp.headers().set(Fields.ContentLength, "0")
    resp
  }
}

/**
 * returns a 413 response to fixed-length messages which exceed the size limit
 */
private[http] class PayloadSizeHandler(limit: StorageUnit, log: Option[Logger])
    extends ChannelInboundHandlerAdapter {

  def this(limit: StorageUnit) = this(limit, None)

  private[this] val limitBytes = limit.inBytes

  // we don't worry about thread-safety because netty guarantees that reads are
  // serialized and on the same-thread.
  private[this] var discarding = false

  import PayloadSizeHandler.mk413

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
    case http: HttpMessage if HttpUtil.getContentLength(http, -1) > limitBytes =>
      discarding = true
      if (http.isInstanceOf[ByteBufHolder]) {
        http.asInstanceOf[ByteBufHolder].release()
      }
      ctx
        .writeAndFlush(mk413(http.protocolVersion))
        .addListener(ChannelFutureListener.CLOSE)

      log match {
        case Some(l) if l.isLoggable(Level.FINE) =>
          l.log(
            Level.FINE,
            s"rejected an oversize payload (${HttpUtil.getContentLength(http)} bytes) from ${ctx.channel.remoteAddress}"
          )
        case _ =>
      }

    // the session is doomed so we reject chunks
    case chunk: HttpContent if discarding =>
      chunk.release()

    case _ =>
      super.channelRead(ctx, msg)
  }
}
