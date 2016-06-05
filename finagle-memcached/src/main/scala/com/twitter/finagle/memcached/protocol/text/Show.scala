package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol._
import com.twitter.io.Buf
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._

/**
 * Used by the server.
 */
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
      Tokens(formatted.map { Buf.ByteArray.Owned(_) })
    case InfoLines(lines) =>
      val statLines = lines map { line =>
        val key = line.key
        val values = line.values
        Tokens(Seq(key) ++ values)
      }
      StatLines(statLines)
    case Values(values) =>
      val tokensWithData = values map {
        case Value(key, value, casUnique, Some(flags)) =>
          TokensWithData(Seq(VALUE, key, flags), value, casUnique)
        case Value(key, value, casUnique, None) =>
          TokensWithData(Seq(VALUE, key, ZERO), value, casUnique)
      }
      ValueLines(tokensWithData)
  }
}

/**
 * Used by the client.
 */
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

  private[this] val ZeroBuf = Buf.Utf8("0")

  private[this] def intToUtf8(i: Int): Buf =
    if (i == 0) ZeroBuf else Buf.Utf8(i.toString)

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = message match {
    case Add(key, flags, expiry, value) =>
      TokensWithData(Seq(ADD, key, intToUtf8(flags), intToUtf8(expiry.inSeconds)), value)
    case Set(key, flags, expiry, value) =>
      TokensWithData(Seq(SET, key, intToUtf8(flags), intToUtf8(expiry.inSeconds)), value)
    case Replace(key, flags, expiry, value) =>
      TokensWithData(Seq(REPLACE, key, intToUtf8(flags), intToUtf8(expiry.inSeconds)), value)
    case Append(key, flags, expiry, value) =>
      TokensWithData(Seq(APPEND, key, intToUtf8(flags), intToUtf8(expiry.inSeconds)), value)
    case Prepend(key, flags, expiry, value) =>
      TokensWithData(Seq(PREPEND, key, intToUtf8(flags), intToUtf8(expiry.inSeconds)), value)
    case Cas(key, flags, expiry, value, casUnique) =>
      TokensWithData(Seq(CAS, key, intToUtf8(flags), intToUtf8(expiry.inSeconds)), value, Some(casUnique))
    case Get(keys) =>
      Tokens(GET +: keys)
    case Gets(keys) =>
      Tokens(GETS +: keys)
    case Getv(keys) =>
      Tokens(GETV +: keys)
    case Upsert(key, flags, expiry, value, version) =>
      TokensWithData(Seq(UPSERT, key, intToUtf8(flags), intToUtf8(expiry.inSeconds)), value, Some(version))
    case Incr(key, amount) =>
      Tokens(Seq(INCR, key, Buf.Utf8(amount.toString)))
    case Decr(key, amount) =>
      Tokens(Seq(DECR, key, Buf.Utf8(amount.toString)))
    case Delete(key) =>
      Tokens(Seq(DELETE, key))
    case Stats(args) => Tokens(STATS +: args)
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
