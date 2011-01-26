package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol.text.{Decoding, Tokens, TokensWithData, ValueLines}
import com.twitter.finagle.kestrel.protocol._
import org.jboss.netty.util.CharsetUtil


class ResponseToEncoding extends OneToOneEncoder {
  private[this] val ZERO          = "0"          : ChannelBuffer
  private[this] val VALUE         = "VALUE"      : ChannelBuffer

  private[this] val STORED        = "STORED"     : ChannelBuffer
  private[this] val NOT_FOUND     = "NOT_FOUND"  : ChannelBuffer
  private[this] val DELETED       = "DELETED"    : ChannelBuffer

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = {
    message match {
      case Stored()       => Tokens(Seq(STORED))
      case Deleted()      => Tokens(Seq(DELETED))
      case NotFound()     => Tokens(Seq(NOT_FOUND))
      case Values(values) =>
        val buffer = ChannelBuffers.dynamicBuffer(100 * values.size)
        val tokensWithData = values map { case Value(key, value) =>
          TokensWithData(Seq(VALUE, key, ZERO), value)
        }
        ValueLines(tokensWithData)
    }
  }
}

class CommandToEncoding extends OneToOneEncoder {
  private[this] val ZERO          = "0": ChannelBuffer

  private[this] val OPEN          = "open"
  private[this] val CLOSE         = "close"
  private[this] val ABORT         = "abort"
  private[this] val PEEK          = "peek"

  private[this] val GET           = "get"    : ChannelBuffer
  private[this] val DELETE        = "delete" : ChannelBuffer
  private[this] val FLUSH         = "flush"  : ChannelBuffer

  private[this] val SET           = "set"    : ChannelBuffer

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = {
    message match {
      case Set(key, expiry, value) =>
        TokensWithData(Seq(SET, key, ZERO, expiry.inSeconds.toString), value)
      case Get(queueName, options) =>
        var key = queueName
        options foreach { option =>
          val optionString = option match {
            case Timeout(timeout) => "t=" + timeout.inSeconds
            case Open() => OPEN
            case Close() => CLOSE
            case Abort() => ABORT
            case Peek() => PEEK
          }
          key += "/" + optionString
        }
        Tokens(Seq(GET, key))
      case Delete(key) =>
        Tokens(Seq(DELETE, key))
      case Flush(key) =>
        Tokens(Seq(FLUSH, key))
    }
  }
}