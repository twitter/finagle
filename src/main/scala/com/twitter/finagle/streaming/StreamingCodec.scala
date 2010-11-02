package com.twitter.finagle.streaming

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.http.{HttpChunk, HttpResponse}
import org.jboss.netty.handler.codec.frame.{DelimiterBasedFrameDecoder, Delimiters}

object StreamingCodec {
  private val delim = Delimiters.lineDelimiter
  private def unframer = new DelimiterBasedFrameDecoder(10<<20, delim(0), delim(1))

  def appendOnPipeline(pipeline: ChannelPipeline) {
    pipeline.addLast("streaming-unframer", unframer)
    pipeline.addLast("streaming-codec", new StreamingCodec)
  }
}

class StreamingCodec extends SimpleChannelUpstreamHandler {
  class StreamingException(message: String) extends Exception(message)
  private val parser = new StatusParserJackson

  private def stringFromUtf8Bytes(bytes: Array[Byte]): String =
    new String(bytes, "UTF-8")

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val text: Option[String] = e.getMessage match {
      case chunk: HttpChunk =>
        Some(stringFromUtf8Bytes(chunk.getContent.array))

      case resp: HttpResponse =>
        if (resp.getStatus().getCode() >= 300) {
          Channels.fireExceptionCaught(ctx, new StreamingException(resp.getStatus.toString))
          None
        } else {
          stringFromUtf8Bytes(resp.getContent.asInstanceOf[ChannelBuffer].array) match {
            case s: String if s.isEmpty => None
            case s: String => Some(s)
          }
        }

      case buf: ChannelBuffer =>
        Some(stringFromUtf8Bytes(buf.array))

      case _ =>
        None
    }

    for (text <- text)
      Channels.fireMessageReceived(
        ctx,
        new CachedMessage(text, parser.parse(text)),
        e.getRemoteAddress)
  }
}
