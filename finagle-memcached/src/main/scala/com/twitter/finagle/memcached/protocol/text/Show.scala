package com.twitter.finagle.memcachedx.protocol.text

import com.twitter.finagle.memcachedx.protocol._
import com.twitter.io.Buf
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.finagle.memcachedx.util.ChannelBufferUtils._
import org.jboss.netty.channel._

class ResponseToEncoding extends OneToOneEncoder {
  private[this] val ZERO          = Buf.Utf8("0")
  private[this] val VALUE         = Buf.Utf8("VALUE")

  private[this] val STORED        = Buf.Utf8("STORED")
  private[this] val NOT_STORED    = Buf.Utf8("NOT_STORED")
  private[this] val EXISTS        = Buf.Utf8("EXISTS")
  private[this] val NOT_FOUND     = Buf.Utf8("NOT_FOUND")
  private[this] val DELETED       = Buf.Utf8("DELETED")

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = message match {
    case Stored()       => Tokens(Seq(STORED))
    case NotStored()    => Tokens(Seq(NOT_STORED))
    case Exists()       => Tokens(Seq(EXISTS))
    case Deleted()      => Tokens(Seq(DELETED))
    case NotFound()     => Tokens(Seq(NOT_FOUND))
    case NoOp()         => Tokens(Nil)
    case Number(value)  => Tokens(Seq(Buf.Utf8(value.toString)))
    case Error(cause)   =>
      val formatted: Seq[Array[Byte]] = ExceptionHandler.format(cause)
      Tokens(formatted.map { Buf.ByteArray(_) })
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
  private[this] val GET           = Buf.Utf8("get")
  private[this] val GETS          = Buf.Utf8("gets")
  private[this] val DELETE        = Buf.Utf8("delete")
  private[this] val INCR          = Buf.Utf8("incr")
  private[this] val DECR          = Buf.Utf8("decr")

  private[this] val ADD           = Buf.Utf8("add")
  private[this] val SET           = Buf.Utf8("set")
  private[this] val APPEND        = Buf.Utf8("append")
  private[this] val PREPEND       = Buf.Utf8("prepend")
  private[this] val REPLACE       = Buf.Utf8("replace")
  private[this] val CAS           = Buf.Utf8("cas")

  private[this] val GETV          = Buf.Utf8("getv")
  private[this] val UPSERT        = Buf.Utf8("upsert")

  private[this] val QUIT          = Buf.Utf8("quit")
  private[this] val STATS         = Buf.Utf8("stats")

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = message match {
    case Add(key, flags, expiry, value) =>
      TokensWithData(Seq(ADD, key, Buf.Utf8(flags.toString), Buf.Utf8(expiry.inSeconds.toString)), value)
    case Set(key, flags, expiry, value) =>
      TokensWithData(Seq(SET, key, Buf.Utf8(flags.toString), Buf.Utf8(expiry.inSeconds.toString)), value)
    case Replace(key, flags, expiry, value) =>
      TokensWithData(Seq(REPLACE, key, Buf.Utf8(flags.toString), Buf.Utf8(expiry.inSeconds.toString)), value)
    case Append(key, flags, expiry, value) =>
      TokensWithData(Seq(APPEND, key, Buf.Utf8(flags.toString), Buf.Utf8(expiry.inSeconds.toString)), value)
    case Prepend(key, flags, expiry, value) =>
      TokensWithData(Seq(PREPEND, key, Buf.Utf8(flags.toString), Buf.Utf8(expiry.inSeconds.toString)), value)
    case Cas(key, flags, expiry, value, casUnique) =>
      TokensWithData(Seq(CAS, key, Buf.Utf8(flags.toString), Buf.Utf8(expiry.inSeconds.toString)), value, Some(casUnique))
    case Get(keys) =>
      Tokens(Seq(GET) ++ keys)
    case Gets(keys) =>
      Tokens(Seq(GETS) ++ keys)
    case Getv(keys) =>
      Tokens(Seq(GETV) ++ keys)
    case Upsert(key, flags, expiry, value, version) =>
      TokensWithData(Seq(UPSERT, key, Buf.Utf8(flags.toString), Buf.Utf8(expiry.inSeconds.toString)), value, Some(version))
    case Incr(key, amount) =>
      Tokens(Seq(INCR, key, Buf.Utf8(amount.toString)))
    case Decr(key, amount) =>
      Tokens(Seq(DECR, key, Buf.Utf8(amount.toString)))
    case Delete(key) =>
      Tokens(Seq(DELETE, key))
    case Stats(args) => Tokens(Seq(STATS) ++ args)
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
