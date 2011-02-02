package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.ClientError
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}
import org.jboss.netty.channel._
import collection.mutable.ArrayBuffer
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.util.CharsetUtil

object AbstractDecoder {
  private val DELIMETER = ChannelBuffers.wrappedBuffer("\r\n".getBytes)
  private val SKIP_SPACE = 1
  private val FIND_CRLF = new ChannelBufferIndexFinder() {
    def find(buffer: ChannelBuffer, guessedIndex: Int): Boolean = {
      if (buffer.writerIndex < guessedIndex + DELIMETER.readableBytes - 1) return false

      val cr = buffer.getByte(guessedIndex)
      val lf = buffer.getByte(guessedIndex + 1)
      cr == '\r' && lf == '\n'
    }
}
}

abstract class AbstractDecoder extends FrameDecoder {
  import AbstractDecoder._
  import ParserUtils._

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    start()
    super.channelOpen(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause.printStackTrace()
    start()
    super.exceptionCaught(ctx, e)
  }

  protected def decodeLine(buffer: ChannelBuffer, needsData: Seq[ChannelBuffer] => Option[Int])(continue: Seq[ChannelBuffer] => Decoding): Decoding = {
    val frameLength = buffer.bytesBefore(FIND_CRLF)
    if (frameLength < 0) {
      needMoreData
    } else {
      val frame = buffer.slice(buffer.readerIndex, frameLength)
      buffer.skipBytes(frameLength + DELIMETER.capacity)

      val tokens = frame.split(" ")
      val bytesNeeded = needsData(tokens)
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
    if (buffer.readableBytes < (bytesNeeded + DELIMETER.capacity))
      needMoreData
    else {
      val lastTwoBytesInFrame = buffer.slice(bytesNeeded + buffer.readerIndex, DELIMETER.capacity)
      if (!lastTwoBytesInFrame.equals(DELIMETER)) throw new ClientError("Missing delimeter")

      val data = buffer.slice(buffer.readerIndex, bytesNeeded)
      buffer.skipBytes(bytesNeeded + DELIMETER.capacity)

      start()
      // Copied rather than wrapped to avoid caching data outside the reader/writer mark.
      continue(ChannelBuffers.copiedBuffer(data))
    }
  }

  private[this] val needMoreData = null

  protected[memcached] def start()
  protected[memcached] def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int)
}