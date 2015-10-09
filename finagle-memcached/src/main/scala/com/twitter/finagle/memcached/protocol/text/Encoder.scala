package com.twitter.finagle.memcached.protocol.text

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.io.Buf

object Encoder {
  private val SPACE         = " ".getBytes
  private val DELIMITER     = "\r\n".getBytes
  private val END           = "END".getBytes
}

class Encoder extends OneToOneEncoder {
  import Encoder._

  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef): ChannelBuffer = {
    message match {
      case Tokens(tokens) =>
        val buffer = ChannelBuffers.dynamicBuffer(10 * tokens.size)
        tokens foreach { token =>
          // Note that certain variants of writeBytes with a
          // ChannelBuffer *consume* the buffer and others *copy* it.
          // This encoder is not supposed to be destructive of the
          // input, so make sure to use a version that does not alter
          // the message.
          //
          // See:
          //  http://netty.io/docs/stable/api/org/jboss/netty/buffer/ChannelBuffer.html#writeBytes(org.jboss.netty.buffer.ChannelBuffer)
          //  http://netty.io/docs/stable/api/org/jboss/netty/buffer/ChannelBuffer.html#writeBytes(org.jboss.netty.buffer.ChannelBuffer,%20int,%20int)

          buffer.writeBytes(BufChannelBuffer(token), 0, token.length)
          buffer.writeBytes(SPACE)
        }
        buffer.writeBytes(DELIMITER)
        buffer
      case TokensWithData(tokens, data, casUnique) =>
        val buffer = ChannelBuffers.dynamicBuffer(50 + data.length)
        tokens foreach { token =>
          buffer.writeBytes(BufChannelBuffer(token), 0, token.length)
          buffer.writeBytes(SPACE)
        }

        buffer.writeBytes(BufChannelBuffer(Buf.Utf8(data.length.toString)))
        casUnique foreach { token =>
          buffer.writeBytes(SPACE)
          buffer.writeBytes(BufChannelBuffer(token), 0, token.length)
        }
        buffer.writeBytes(DELIMITER)
        buffer.writeBytes(BufChannelBuffer(data), 0, data.length)
        buffer.writeBytes(DELIMITER)
        buffer
      case ValueLines(lines) =>
        val buffer = ChannelBuffers.dynamicBuffer(100 * lines.size)
        lines foreach { case TokensWithData(tokens, data, casUnique) =>
          tokens foreach { token =>
            buffer.writeBytes(BufChannelBuffer(token), 0, token.length)
            buffer.writeBytes(SPACE)
          }
          buffer.writeBytes(BufChannelBuffer(Buf.Utf8(data.length.toString)))
          casUnique foreach { token =>
            buffer.writeBytes(SPACE)
            buffer.writeBytes(BufChannelBuffer(token), 0, token.length)
          }
          buffer.writeBytes(DELIMITER)
          buffer.writeBytes(BufChannelBuffer(data), 0, data.length)
          buffer.writeBytes(DELIMITER)
        }
        buffer.writeBytes(END)
        buffer.writeBytes(DELIMITER)
        buffer
      case StatLines(lines) =>
        val buffer = ChannelBuffers.dynamicBuffer(100 * lines.size)
        lines foreach { case Tokens(tokens) =>
          tokens foreach { token =>
            buffer.writeBytes(BufChannelBuffer(token), 0, token.length)
            buffer.writeBytes(SPACE)
          }
          buffer.writeBytes(DELIMITER)
        }
        buffer.writeBytes(END)
        buffer.writeBytes(DELIMITER)
        buffer
    }
  }
}
