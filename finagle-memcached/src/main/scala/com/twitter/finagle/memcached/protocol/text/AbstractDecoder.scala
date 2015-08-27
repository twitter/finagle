package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.io.Charsets
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBufferIndexFinder, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder

object AbstractDecoder {
  private val Delimiter = ChannelBuffers.wrappedBuffer("\r\n".getBytes(Charsets.Utf8))
  private val DelimiterLength = Delimiter.capacity
  private val FindCRLF = new ChannelBufferIndexFinder() {
    def find(buffer: ChannelBuffer, guessedIndex: Int): Boolean = {
      val enoughBytesForDelimeter = guessedIndex + Delimiter.readableBytes
      if (buffer.writerIndex < enoughBytesForDelimeter) return false

      buffer.getByte(guessedIndex) == '\r' &&
        buffer.getByte(guessedIndex + 1) == '\n'
    }
  }
}

abstract class AbstractDecoder extends FrameDecoder {
  import AbstractDecoder._

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    start()
    super.channelOpen(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
    start()
    super.exceptionCaught(ctx, e)
  }

  /**
   * @param needsData return the number of bytes needed, or `-1` if no more bytes
   *                  are necessary.
   */
  protected def decodeLine(
    buffer: ChannelBuffer,
    needsData: Seq[ChannelBuffer] => Int
  )(continue: Seq[ChannelBuffer] => Decoding
  ): Decoding = {
    val frameLength = buffer.bytesBefore(FindCRLF)
    if (frameLength < 0) {
      null
    } else {
      val frame = buffer.slice(buffer.readerIndex, frameLength)
      buffer.skipBytes(frameLength + DelimiterLength)

      val tokens = frame.split
      val bytesNeeded = if (tokens.nonEmpty) needsData(tokens) else -1
      if (bytesNeeded == -1) {
        start()
        continue(tokens)
      } else {
        awaitData(tokens, bytesNeeded)
        null
      }
    }
  }

  protected def decodeData(
    bytesNeeded: Int,
    buffer: ChannelBuffer
  )(continue: ChannelBuffer => Decoding
  ): Decoding = {
    if (buffer.readableBytes < (bytesNeeded + DelimiterLength))
      null
    else {
      if (!FindCRLF.find(buffer, bytesNeeded + buffer.readerIndex))
        throw new ClientError("Missing delimiter")

      val data = buffer.slice(buffer.readerIndex, bytesNeeded)
      buffer.skipBytes(bytesNeeded + DelimiterLength)

      start()
      // Shared rather than wrapped to avoid caching data outside the reader/writer mark.
      continue(ChannelBuffers.copiedBuffer(data))
    }
  }

  protected[memcached] def start(): Unit
  protected[memcached] def awaitData(tokens: Seq[ChannelBuffer], bytesNeeded: Int): Unit
}
