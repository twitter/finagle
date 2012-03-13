package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.text._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.ChannelBuffers
import java.net.InetSocketAddress
import com.twitter.finagle.tracing.Trace

object Encoder {
  private val SPACE         = " ".getBytes
  private val DELIMITER     = "\r\n".getBytes
  private val END           = "END".getBytes
}

class Encoder extends OneToOneEncoder {
  import Encoder._

  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    // set the addr for this trace
    context.getChannel.getLocalAddress() match {
      case ia: InetSocketAddress => Trace.recordClientAddr(ia)
      case _ => () // nothing
    }
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

          buffer.writeBytes(token, 0, token.readableBytes)
          buffer.writeBytes(SPACE)
        }
        buffer.writeBytes(DELIMITER)
        buffer
      case TokensWithData(tokens, data, casUnique) =>
        val buffer = ChannelBuffers.dynamicBuffer(50 + data.readableBytes)
        tokens foreach { token =>
          buffer.writeBytes(token, 0, token.readableBytes)
          buffer.writeBytes(SPACE)
        }
        buffer.writeBytes(data.readableBytes.toString.getBytes)
        casUnique foreach { token =>
          buffer.writeBytes(SPACE)
          buffer.writeBytes(token, 0, token.readableBytes)
        }
        buffer.writeBytes(DELIMITER)
        buffer.writeBytes(data, 0, data.readableBytes)
        buffer.writeBytes(DELIMITER)
        buffer
      case ValueLines(lines) =>
        val buffer = ChannelBuffers.dynamicBuffer(100 * lines.size)
        lines foreach { case TokensWithData(tokens, data, casUnique) =>
          tokens foreach { token =>
            buffer.writeBytes(token, 0, token.readableBytes)
            buffer.writeBytes(SPACE)
          }
          buffer.writeBytes(data.readableBytes.toString.getBytes)
          casUnique foreach { token =>
            buffer.writeBytes(SPACE)
            buffer.writeBytes(token, 0, token.readableBytes)
          }
          buffer.writeBytes(DELIMITER)
          buffer.writeBytes(data, 0, data.readableBytes)
          buffer.writeBytes(DELIMITER)
        }
        buffer.writeBytes(END)
        buffer.writeBytes(DELIMITER)
        buffer
      case StatLines(lines) =>
        val buffer = ChannelBuffers.dynamicBuffer(100 * lines.size)
        lines foreach { case Tokens(tokens) =>
          tokens foreach { token =>
            buffer.writeBytes(token, 0, token.readableBytes)
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
