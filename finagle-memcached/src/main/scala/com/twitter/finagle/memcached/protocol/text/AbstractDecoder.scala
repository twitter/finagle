package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.ClientError
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}
import org.jboss.netty.channel._
import org.jboss.netty.util.CharsetUtil

abstract class AbstractDecoder[A] extends FrameDecoder {
  private[this] val DELIMETER = ChannelBuffers.wrappedBuffer("\r\n".getBytes)

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    start()
    super.channelOpen(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause.printStackTrace()
    start()
    super.exceptionCaught(ctx, e)
  }

  protected def decodeLine(buffer: ChannelBuffer, needsData: Seq[ChannelBuffer] => Option[Int])(parse: Seq[ChannelBuffer] => A): A = {
    val frameLength = buffer.bytesBefore(ChannelBufferIndexFinder.CRLF)
    if (frameLength < 0) {
      needMoreData
    } else {
      val frame = buffer.slice(buffer.readerIndex, frameLength)
      buffer.skipBytes(frameLength + DELIMETER.capacity)

      val tokens = Parser.tokenize(frame)
      val bytesNeeded = needsData(tokens)
      if (bytesNeeded.isDefined) {
        awaitData(tokens, bytesNeeded.get)
      } else {
        start()
        parse(tokens)
      }
    }
  }

  protected def decodeData(bytesNeeded: Int, buffer: ChannelBuffer)(parse: (ChannelBuffer) => A): A = {
    if (buffer.readableBytes < (bytesNeeded + DELIMETER.capacity))
      needMoreData
    else {
      val lastTwoBytesInFrame = buffer.slice(bytesNeeded + buffer.readerIndex, DELIMETER.capacity)
      if (!lastTwoBytesInFrame.equals(DELIMETER)) throw new ClientError("Missing delimeter")

      val data = buffer.slice(buffer.readerIndex, bytesNeeded)
      buffer.skipBytes(bytesNeeded + DELIMETER.capacity)

      start()
      parse(ChannelBuffers.copiedBuffer(data))
    }
  }

  protected def start()
  protected def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int): A
  protected val needMoreData: A
}