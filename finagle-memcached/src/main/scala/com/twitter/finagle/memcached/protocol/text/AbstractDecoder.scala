package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.ClientError
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._

object AbstractDecoder {
  private val Delimiter = ChannelBuffers.wrappedBuffer("\r\n".getBytes)
  private val DelimiterLength = Delimiter.capacity
  private val FindCRLF = new ChannelBufferIndexFinder() {
    def find(buffer: ChannelBuffer, guessedIndex: Int): Boolean = {
      val enoughBytesForDelimeter = guessedIndex + Delimiter.readableBytes
      if (buffer.writerIndex < enoughBytesForDelimeter) return false

      val cr = buffer.getByte(guessedIndex)
      val lf = buffer.getByte(guessedIndex + 1)
      cr == '\r' && lf == '\n'
    }
  }
}

abstract class AbstractDecoder extends FrameDecoder {
  import AbstractDecoder._

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    start()
    super.channelOpen(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    start()
    super.exceptionCaught(ctx, e)
  }

  protected def decodeLine(buffer: ChannelBuffer, needsData: Seq[ChannelBuffer] => Option[Int])(continue: Seq[ChannelBuffer] => Decoding): Decoding = {
    val frameLength = buffer.bytesBefore(FindCRLF)
    if (frameLength < 0) {
      needMoreData
    } else {
      val frame = buffer.slice(buffer.readerIndex, frameLength)
      buffer.skipBytes(frameLength + DelimiterLength)

      val tokens = frame.split
      val bytesNeeded = if (tokens.length > 0) needsData(tokens) else None
      if (bytesNeeded.isDefined) {
        awaitData(tokens, bytesNeeded.get)
        needMoreData
      } else {
        start()
        continue(tokens)
      }
    }
  }

  protected def decodeData(bytesNeeded: Int, buffer: ChannelBuffer)(continue: ChannelBuffer => Decoding): Decoding = {
    if (buffer.readableBytes < (bytesNeeded + DelimiterLength))
      needMoreData
    else {
      val lastTwoBytesInFrame = buffer.slice(bytesNeeded + buffer.readerIndex, DelimiterLength)
      if (!lastTwoBytesInFrame.equals(Delimiter)) throw new ClientError("Missing delimiter")

      val data = buffer.slice(buffer.readerIndex, bytesNeeded)
      buffer.skipBytes(bytesNeeded + DelimiterLength)

      start()
      // Copied rather than wrapped to avoid caching data outside the reader/writer mark.
      continue(ChannelBuffers.copiedBuffer(data))
    }
  }

  private[this] val needMoreData = null

  protected[memcached] def start()
  protected[memcached] def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int)
}
