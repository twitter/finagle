package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.channel._


class ResponseToEncoding extends OneToOneEncoder {
  private[this] val ZERO          = "0"
  private[this] val VALUE         = "VALUE"

  private[this] val STORED        = "STORED"
  private[this] val NOT_STORED    = "NOT_STORED"
  private[this] val EXISTS        = "EXISTS"
  private[this] val NOT_FOUND     = "NOT_FOUND"
  private[this] val DELETED       = "DELETED"

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
  private[this] val GET           = "get"
  private[this] val DELETE        = "delete"
  private[this] val INCR          = "incr"
  private[this] val DECR          = "decr"

  private[this] val ADD           = "add"
  private[this] val SET           = "set"
  private[this] val APPEND        = "append"
  private[this] val PREPEND       = "prepend"
  private[this] val REPLACE       = "replace"

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
      Tokens(Seq[ChannelBuffer](GET) ++ keys)
    case Incr(key, amount) =>
      Tokens(Seq(INCR, key, amount.toString))
    case Decr(key, amount) =>
      Tokens(Seq(DECR, key, amount.toString))
    case Delete(key) =>
      Tokens(Seq(DELETE, key))
  }
}

class ExceptionHandler extends SimpleChannelUpstreamHandler {
  private[this] val DELIMITER     = "\r\n"
  private[this] val ERROR         = copiedBuffer("ERROR",        DELIMITER)
  private[this] val CLIENT_ERROR  = copiedBuffer("CLIENT_ERROR", DELIMITER)
  private[this] val SERVER_ERROR  = copiedBuffer("SERVER_ERROR", DELIMITER)

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