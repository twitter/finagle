package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffers}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.channel._
import com.twitter.finagle.memcached.protocol.text.{Decoding, Tokens, TokensWithData, ValueLines}
import com.twitter.finagle.kestrel.protocol._
import org.jboss.netty.util.CharsetUtil


private[kestrel] class ResponseToEncoding extends OneToOneEncoder {
  private[this] val ZERO          = "0"
  private[this] val VALUE         = "VALUE"

  private[this] val STORED        = "STORED"
  private[this] val NOT_FOUND     = "NOT_FOUND"
  private[this] val DELETED       = "DELETED"

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

private[kestrel] class CommandToEncoding extends OneToOneEncoder {
  private[this] val ZERO          = "0"

  private[this] val OPEN          = "open"
  private[this] val CLOSE         = "close"
  private[this] val ABORT         = "abort"
  private[this] val PEEK          = "peek"

  private[this] val GET           = "get"
  private[this] val DELETE        = "delete"
  private[this] val FLUSH         = "flush"

  private[this] val SET           = "set"

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = {
    message match {
      case Set(key, expiry, value) =>
        TokensWithData(Seq(SET, key, ZERO, expiry.inSeconds.toString), value)
      case Get(queueName, options) =>
        var key = queueName.toString(CharsetUtil.US_ASCII)
        options foreach { option =>
          val optionString = option match {
            case Timeout(timeout) => "t=" + timeout.inMilliseconds.toString
            case Open()  => OPEN
            case Close() => CLOSE
            case Abort() => ABORT
            case Peek()  => PEEK
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