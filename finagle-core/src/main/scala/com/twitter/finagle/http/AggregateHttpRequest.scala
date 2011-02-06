package com.twitter.finagle.http

/**
 * Aggregate HTTP chunked HTTP requests (ie. POSTs with 100-continue)
 */

import scala.collection.JavaConversions._

import java.nio.ByteOrder

import org.jboss.netty.channel.{
  MessageEvent, Channels, SimpleChannelUpstreamHandler,
  ChannelHandlerContext}
import org.jboss.netty.handler.codec.http.{
  HttpHeaders, HttpRequest, HttpResponse, HttpChunk,
  DefaultHttpResponse, HttpVersion, HttpResponseStatus}
import org.jboss.netty.buffer.{
  ChannelBuffer, ChannelBuffers,
  CompositeChannelBuffer}

import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.channel.LeftFoldUpstreamHandler

object OneHundredContinueResponse
  extends DefaultHttpResponse(
    HttpVersion.HTTP_1_1,
    HttpResponseStatus.CONTINUE)

class HttpFailure(ctx: ChannelHandlerContext, status: HttpResponseStatus)
  extends LeftFoldUpstreamHandler
{
  { // TODO: always transition to an untenable state
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)
    val future = Channels.future(ctx.getChannel)
    Channels.write(ctx, future, response, ctx.getChannel.getRemoteAddress)    
    future onSuccessOrFailure { ctx.getChannel.close() }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) =
    this  // (swallow the message)
}

case class AggregateHttpChunks(
    whenDone: LeftFoldUpstreamHandler,
    request: HttpRequest,
    maxSize: Int,
    bytesSoFar: Int = 0,
    chunks: List[ChannelBuffer] = Nil)
  extends LeftFoldUpstreamHandler
{
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) =
    e.getMessage match {
      case chunk: HttpChunk =>
        val chunkBuffer = chunk.getContent
        val chunkBufferSize = chunkBuffer.readableBytes
        if (bytesSoFar + chunkBufferSize > maxSize) {
          new HttpFailure(ctx, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)
        } else if (chunk.isLast) {
          // Remove traces of request chunking.
          val encodings = request.getHeaders(HttpHeaders.Names.TRANSFER_ENCODING)
          encodings.remove(HttpHeaders.Values.CHUNKED)
          if (encodings.isEmpty())
            request.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
          request.setChunked(false)

          // Set the content
          val compositeBuffer =
            new CompositeChannelBuffer(ByteOrder.BIG_ENDIAN, (chunkBuffer :: chunks).reverse)
          request.setContent(compositeBuffer)

          // And let it on its way.
          Channels.fireMessageReceived(ctx, request)

          // Transition back to initial state.
          whenDone
        } else {
          copy(bytesSoFar = bytesSoFar + chunkBufferSize,
               chunks     = chunkBuffer :: chunks)
        }

      case _ =>
        new HttpFailure(ctx, HttpResponseStatus.BAD_REQUEST)
    }
}

class AggregateHttpRequest(maxBufferSize: Int)
  extends LeftFoldUpstreamHandler
{
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) =
    e.getMessage match {
      case request: HttpRequest if HttpHeaders.is100ContinueExpected(request) =>
        if (HttpHeaders.getContentLength(request, -1L) > maxBufferSize) {
          new HttpFailure(ctx, HttpResponseStatus.EXPECTATION_FAILED)
        } else {
          val future = Channels.future(ctx.getChannel)
          Channels.write(ctx, future, OneHundredContinueResponse, e.getRemoteAddress)
          request.removeHeader(HttpHeaders.Names.EXPECT)
          new AggregateHttpChunks(this, request, maxBufferSize)
        }

      case _ =>
        super.messageReceived(ctx, e)
    }
}
