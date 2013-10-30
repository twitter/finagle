package com.twitter.finagle.http.codec

import com.twitter.finagle.http.Response
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.io.Buf
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{DefaultHttpChunk, HttpChunk, HttpHeaders}
import org.jboss.netty.channel.{Channels, ChannelFuture, ChannelFutureListener,
  ChannelHandlerContext, MessageEvent, SimpleChannelDownstreamHandler}


/**
 * Convert Finagle-HTTP Responses to Netty responses
 */
class ResponseEncoder extends SimpleChannelDownstreamHandler {

  def channelsWrite(
    ctx: ChannelHandlerContext,
    writeFuture: ChannelFuture,
    message: Object,
    remoteAddr: SocketAddress
  ) {
    Channels.write(ctx, writeFuture, message, remoteAddr)
  }

  private[this] def continue(
    ctx: ChannelHandlerContext,
    future: ChannelFuture,
    response: Response,
    remoteAddr: SocketAddress
  ) {
    (response.reader.read(Int.MaxValue) map toHttpChunk) onSuccess { chunk =>
      val writeFuture = Channels.future(ctx.getChannel())
      channelsWrite(ctx, writeFuture, chunk, remoteAddr)
      writeFuture.addListener(new ChannelFutureListener() {
        def operationComplete(f: ChannelFuture) {
          if (f.isCancelled()) future.cancel()
          else if (!f.isSuccess()) {
            response.reader.discard()
            future.setFailure(f.getCause())
          }
          else if (chunk.isLast()) future.setSuccess()
          else continue(ctx, future, response, remoteAddr)
        }
      })
    } onFailure { t =>
      future.setFailure(t)
    }
  }

  private[this] def writeResponse(
    ctx: ChannelHandlerContext,
    future: ChannelFuture,
    response: Response,
    remoteAddr: SocketAddress
  ) {
    val writeFuture = Channels.future(ctx.getChannel())
    channelsWrite(ctx, writeFuture, response, remoteAddr)
    writeFuture.addListener(new ChannelFutureListener() {
      def operationComplete(f: ChannelFuture) {
        if (f.isCancelled()) future.cancel()
        else if (!f.isSuccess()) {
          response.reader.discard()
          future.setFailure(f.getCause())
        }
        else continue(ctx, future, response, remoteAddr)
      }
    })
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case response: Response =>
        if (response.isChunked) {
          writeResponse(ctx, e.getFuture(), response, e.getRemoteAddress())
        } else {
          // Ensure Content-Length is set if not chunked
          if (!response.containsHeader(HttpHeaders.Names.CONTENT_LENGTH)) {
            response.contentLength = response.getContent().readableBytes
          }
          super.writeRequested(ctx, e)
        }
      case _ =>
        super.writeRequested(ctx, e)
    }
  }

  private[this] def toHttpChunk(buf: Buf): HttpChunk = buf match {
    case Buf.Eof => HttpChunk.LAST_CHUNK
    case cbBuf: ChannelBufferBuf => new DefaultHttpChunk(cbBuf.buf)
    case _ =>
      val bytes = new Array[Byte](buf.length)
      buf.write(bytes, 0)
      new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(bytes))
  }
}
