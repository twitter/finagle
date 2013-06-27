package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
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
    case Exists()       => Tokens(Seq(EXISTS))
    case Deleted()      => Tokens(Seq(DELETED))
    case NotFound()     => Tokens(Seq(NOT_FOUND))
    case NoOp()         => Tokens(Nil)
    case Number(value)  => Tokens(Seq(value.toString))
    case Error(cause)   =>
      val formatted = ExceptionHandler.format(cause)
      Tokens(formatted.map { ChannelBuffers.copiedBuffer(_) })
    case InfoLines(lines) =>
      val buffer = ChannelBuffers.dynamicBuffer(100 * lines.size)
      val statLines = lines map { line =>
        val key = line.key
        val values = line.values
        Tokens(Seq(key) ++ values)
      }
      StatLines(statLines)
    case Values(values) =>
      val buffer = ChannelBuffers.dynamicBuffer(100 * values.size)
      val tokensWithData = values map {
        case Value(key, value, casUnique, Some(flags)) =>
          TokensWithData(Seq(VALUE, key, flags), value, casUnique)
        case Value(key, value, casUnique, None) =>
          TokensWithData(Seq(VALUE, key, ZERO), value, casUnique)
      }
      ValueLines(tokensWithData)
  }
}

class CommandToEncoding extends OneToOneEncoder {
  private[this] val GET           = "get"
  private[this] val GETS          = "gets"
  private[this] val DELETE        = "delete"
  private[this] val INCR          = "incr"
  private[this] val DECR          = "decr"

  private[this] val ADD           = "add"
  private[this] val SET           = "set"
  private[this] val APPEND        = "append"
  private[this] val PREPEND       = "prepend"
  private[this] val REPLACE       = "replace"
  private[this] val CAS           = "cas"

  private[this] val GETV          = "getv"
  private[this] val UPSERT        = "upsert"

  private[this] val QUIT          = "quit"
  private[this] val STATS         = "stats"

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
    case Cas(key, flags, expiry, value, casUnique) =>
      TokensWithData(Seq(CAS, key, flags.toString, expiry.inSeconds.toString), value, Some(casUnique))
    case Get(keys) =>
      encode(ctx, ch, Gets(keys))
    case Gets(keys) =>
      Tokens(Seq[ChannelBuffer](GETS) ++ keys)
    case Getv(keys) =>
      Tokens(Seq[ChannelBuffer](GETV) ++ keys)
    case Upsert(key, flags, expiry, value, version) =>
      TokensWithData(Seq(UPSERT, key, flags.toString, expiry.inSeconds.toString), value, Some(version))
    case Incr(key, amount) =>
      Tokens(Seq(INCR, key, amount.toString))
    case Decr(key, amount) =>
      Tokens(Seq(DECR, key, amount.toString))
    case Delete(key) =>
      Tokens(Seq(DELETE, key))
    case Stats(args) => Tokens(Seq[ChannelBuffer](STATS) ++ args)
    case Quit() =>
      Tokens(Seq(QUIT))
  }
}

class ExceptionHandler extends SimpleChannelUpstreamHandler {
  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = {
    val formatted = ExceptionHandler.formatWithEol(e.getCause)
    Channels.write(ctx.getChannel, ChannelBuffers.copiedBuffer(formatted:_*))
  }
}

object ExceptionHandler {
  private val DELIMITER     = "\r\n".getBytes
  private val ERROR         = "ERROR".getBytes
  private val CLIENT_ERROR  = "CLIENT_ERROR".getBytes
  private val SERVER_ERROR  = "SERVER_ERROR".getBytes
  private val SPACE         = " ".getBytes
  private val Newlines      = "[\\r\\n]".r

  def formatWithEol(e: Throwable) = format(e) match {
    case head :: Nil => Seq(head, DELIMITER)
    case head :: tail :: Nil => Seq(head, SPACE, tail, DELIMITER)
    case _ => throw e
  }

  def format(e: Throwable) = e match {
    case e: NonexistentCommand =>
      Seq(ERROR)
    case e: ClientError        =>
      Seq(CLIENT_ERROR, Newlines.replaceAllIn(e.getMessage, " ").getBytes)
    case e: ServerError        =>
      Seq(SERVER_ERROR, Newlines.replaceAllIn(e.getMessage, " ").getBytes)
    case t                     =>
      throw t
  }
}
