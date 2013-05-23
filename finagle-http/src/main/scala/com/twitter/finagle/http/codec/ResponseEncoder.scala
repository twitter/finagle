package com.twitter.finagle.http.codec

import com.twitter.finagle.http.Response
import java.net.SocketAddress
import org.jboss.netty.channel.{Channels, ChannelFuture, ChannelFutureListener, ChannelHandlerContext, MessageEvent,
  SimpleChannelDownstreamHandler}
import org.jboss.netty.handler.codec.http.HttpHeaders


/**
 * Convert Finagle-HTTP Responses to Netty responses
 */
class ResponseEncoder extends SimpleChannelDownstreamHandler {

  private[this] def continue(ctx: ChannelHandlerContext,
                             future: ChannelFuture,
                             response: Response,
                             remoteAddr: SocketAddress) {
    response.readChunk() onSuccess { chunk =>
      val writeFuture = Channels.future(ctx.getChannel())
      Channels.write(ctx, writeFuture, chunk, remoteAddr)
      writeFuture.addListener(new ChannelFutureListener() {
        def operationComplete(f: ChannelFuture) {
          if (f.isCancelled()) future.cancel()
          else if (!f.isSuccess()) future.setFailure(f.getCause())
          else if (chunk.isLast()) future.setSuccess()
          else continue(ctx, future, response, remoteAddr)
        }
      })
    }
  }

  private[this] def writeResponse(ctx: ChannelHandlerContext,
                                  future: ChannelFuture,
                                  response: Response,
                                  remoteAddr: SocketAddress) {
    val writeFuture = Channels.future(ctx.getChannel())
    Channels.write(ctx, writeFuture, response, remoteAddr)
    writeFuture.addListener(new ChannelFutureListener() {
      def operationComplete(f: ChannelFuture) {
        if (f.isCancelled()) future.cancel()
        else if (!f.isSuccess()) future.setFailure(f.getCause())
        else continue(ctx, future, response, remoteAddr)
      }
    })
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case response: Response =>
        if (response.isChunked) {
          writeResponse(ctx, e.getFuture(), response, e.getRemoteAddress())
        // Ensure Content-Length is set if not chunked
        } else if (!response.containsHeader(HttpHeaders.Names.CONTENT_LENGTH)) {
          response.contentLength = response.getContent().readableBytes
          super.writeRequested(ctx, e)
        }
      case _ =>
        super.writeRequested(ctx, e)
    }
  }
}
