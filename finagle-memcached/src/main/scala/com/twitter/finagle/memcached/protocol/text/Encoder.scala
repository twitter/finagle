package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.text._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.buffer.ChannelBuffers

object Encoder {
  private val SPACE         = " ".getBytes
  private val DELIMETER     = "\r\n".getBytes
  private val END           = "END".getBytes
}

class Encoder extends OneToOneEncoder {
  import Encoder._

  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case Tokens(tokens) =>
        val buffer = ChannelBuffers.dynamicBuffer(10 * tokens.size)
        tokens foreach { token =>
          buffer.writeBytes(token)
          buffer.writeBytes(SPACE)
        }
        buffer.writeBytes(DELIMETER)
        buffer
      case TokensWithData(tokens, data) =>
        val buffer = ChannelBuffers.dynamicBuffer(50 + data.readableBytes)
        tokens foreach { token =>
          buffer.writeBytes(token)
          buffer.writeBytes(SPACE)
        }
        buffer.writeBytes(data.readableBytes.toString.getBytes)
        buffer.writeBytes(DELIMETER)
        buffer.writeBytes(data)
        buffer.writeBytes(DELIMETER)
        buffer
      case ValueLines(lines) =>
        val buffer = ChannelBuffers.dynamicBuffer(100 * lines.size)
        lines foreach { case TokensWithData(tokens, data) =>
          tokens foreach { token =>
            buffer.writeBytes(token)
            buffer.writeBytes(SPACE)
          }
          buffer.writeBytes(data.readableBytes.toString.getBytes)
          buffer.writeBytes(DELIMETER)
          buffer.writeBytes(data)
          buffer.writeBytes(DELIMETER)
        }
        buffer.writeBytes(END)
        buffer.writeBytes(DELIMETER)
        buffer
    }
  }
}