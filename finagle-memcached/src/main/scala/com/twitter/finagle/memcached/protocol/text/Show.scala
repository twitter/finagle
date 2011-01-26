package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.channel._


class ResponseToEncoding extends OneToOneEncoder {
  private[this] val ZERO          = "0"          : ChannelBuffer
  private[this] val VALUE         = "VALUE"      : ChannelBuffer

  private[this] val STORED        = "STORED"     : ChannelBuffer
  private[this] val NOT_STORED    = "NOT_STORED" : ChannelBuffer
  private[this] val EXISTS        = "EXISTS"     : ChannelBuffer
  private[this] val NOT_FOUND     = "NOT_FOUND"  : ChannelBuffer
  private[this] val DELETED       = "DELETED"    : ChannelBuffer

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = message match {
    case Stored()       => Tokens(Seq(STORED))
    case NotStored()    => Tokens(Seq(NOT_STORED))
    case Deleted()      => Tokens(Seq(DELETED))
    case NotFound()     => Tokens(Seq(NOT_FOUND))
    case Number(value)  => Tokens(Seq(value.toString))
    case Values(values) =>
      val buffer = ChannelBuffers.dynamicBuffer(100 * values.size)
      val tokensWithData = values map { case Value(key, value) =>
        TokensWithData(Seq(VALUE, key, ZERO), value)
      }
      ValueLines(tokensWithData)
  }
}

class CommandToEncoding extends OneToOneEncoder {
  private[this] val GET           = "get"    : ChannelBuffer
  private[this] val DELETE        = "delete" : ChannelBuffer
  private[this] val INCR          = "incr"   : ChannelBuffer
  private[this] val DECR          = "decr"   : ChannelBuffer

  private[this] val ADD           = "add"    : ChannelBuffer
  private[this] val SET           = "set"    : ChannelBuffer
  private[this] val APPEND        = "append" : ChannelBuffer
  private[this] val PREPEND       = "prepend": ChannelBuffer
  private[this] val REPLACE       = "replace": ChannelBuffer

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = message match {
    case Add(key, flags, expiry, value) =>
      TokensWithData(Seq(ADD, key, flags.toString, expiry.inSeconds.toString), value)
    case Set(key, flags, expiry, value) =>
      TokensWithData(Seq(SET, key, flags.toString, expiry.inSeconds.toString), value)
    case Replace(key, flags, expiry, value) =>
      TokensWithData(Seq(REPLACE, key, flags.toString, expiry.inSeconds.toString), value)
    case Append(key, flags, expiry, value) =>
      TokensWithData(Seq(APPEND, key, flags.toString, expiry.inSeconds.toString), value)
    case Prepend(key, flags, expiry, value) =>
      TokensWithData(Seq(PREPEND, key, flags.toString, expiry.inSeconds.toString), value)
    case Get(keys) =>
      encode(ctx, ch, Gets(keys))
    case Gets(keys) =>
      Tokens(Seq(GET) ++ keys)
    case Incr(key, amount) =>
      Tokens(Seq(DECR, key, amount.toString))
    case Decr(key, amount) =>
      Tokens(Seq(DECR, key, amount.toString))
    case Delete(key) =>
      Tokens(Seq(DELETE, key))
  }
}

class ExceptionHandler extends SimpleChannelUpstreamHandler {
  private[this] val DELIMETER     = "\r\n"   .getBytes
  private[this] val ERROR         = copiedBuffer("ERROR".getBytes,        DELIMETER)
  private[this] val CLIENT_ERROR  = copiedBuffer("CLIENT_ERROR".getBytes, DELIMETER)
  private[this] val SERVER_ERROR  = copiedBuffer("SERVER_ERROR".getBytes, DELIMETER)

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = {
    e.getCause match {
      case e: NonexistentCommand =>
        Channels.write(ctx.getChannel, ERROR)
      case e: ClientError        =>
        Channels.write(ctx.getChannel, CLIENT_ERROR)
      case e: ServerError        =>
        Channels.write(ctx.getChannel, SERVER_ERROR)
      case t                     =>
        throw t
    }
  }
}