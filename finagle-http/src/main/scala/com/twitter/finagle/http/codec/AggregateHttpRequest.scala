package com.twitter.finagle.http.codec

/**
 * Aggregate HTTP chunked HTTP requests (ie. POSTs with 100-continue)
 */


import org.jboss.netty.channel.{
  MessageEvent, Channels, ChannelHandlerContext}
import org.jboss.netty.handler.codec.http.{
  HttpHeaders, HttpRequest,
  HttpChunk, DefaultHttpResponse,
  HttpVersion, HttpResponseStatus}
import org.jboss.netty.buffer.{
  ChannelBuffer, ChannelBuffers}

import com.twitter.finagle.netty3.Conversions._
import com.twitter.finagle.channel.LeftFoldUpstreamHandler


private[finagle] class HttpFailure(ctx: ChannelHandlerContext, status: HttpResponseStatus)
  extends LeftFoldUpstreamHandler
{
  {
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
    bufferBudget: Int,
    buffer: ChannelBuffer = ChannelBuffers.EMPTY_BUFFER)
  extends LeftFoldUpstreamHandler
{
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) =
    e.getMessage match {
      case chunk: HttpChunk =>
        val chunkBuffer = chunk.getContent
        val chunkBufferSize = chunkBuffer.readableBytes

        if (chunkBufferSize > bufferBudget) {
          new HttpFailure(ctx, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE)
        } else if (chunk.isLast) {
          // Remove traces of request chunking.
          val encodings = request.getHeaders(HttpHeaders.Names.TRANSFER_ENCODING)
          encodings.remove(HttpHeaders.Values.CHUNKED)
          if (encodings.isEmpty)
            request.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING)

          request.setChunked(false)

          // Set the content
          request.setContent(ChannelBuffers.wrappedBuffer(buffer, chunkBuffer))

          // And let it on its way.
          Channels.fireMessageReceived(ctx, request)

          // Transition back to initial state.
          whenDone
        } else {
          copy(bufferBudget = bufferBudget - chunkBufferSize,
               buffer       = ChannelBuffers.wrappedBuffer(buffer, chunkBuffer))
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

          // Remove the the ``Expect:'' header and continue with
          // collecting chunks.
          request.removeHeader(HttpHeaders.Names.EXPECT)
          AggregateHttpChunks(this, request, maxBufferSize)
        }

      case _ =>
        super.messageReceived(ctx, e)
    }
}
