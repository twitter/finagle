package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

import com.twitter.finagle.memcached.protocol.text.{Decoding, Tokens, TokensWithData, ValueLines}
import com.twitter.io.Buf
import com.twitter.util.{Time, Duration}

object ResponseToEncoding {
  private val ZERO          = "0"
  private val VALUE         = "VALUE"
  private val ZeroCb        = Buf.Utf8(ZERO)
  private val ValueCb       = Buf.Utf8(VALUE)

  private val STORED        = "STORED"
  private val NOT_FOUND     = "NOT_FOUND"
  private val DELETED       = "DELETED"
  private val ERROR         = "ERROR"

  private val StoredTokens   = Tokens(Seq(Buf.Utf8(STORED)))
  private val NotFoundTokens = Tokens(Seq(Buf.Utf8(NOT_FOUND)))
  private val DeletedTokens  = Tokens(Seq(Buf.Utf8(DELETED)))
  private val ErrorTokens    = Tokens(Seq(Buf.Utf8(ERROR)))
}

private[kestrel] class ResponseToEncoding extends OneToOneEncoder {
  import ResponseToEncoding._

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = {
    message match {
      case Stored()       => StoredTokens
      case Deleted()      => DeletedTokens
      case NotFound()     => NotFoundTokens
      case Error()        => ErrorTokens
      case Values(values) =>
        val tokensWithData = values map { case Value(key, value) =>
          TokensWithData(Seq(ValueCb, key, ZeroCb), value)
        }
        ValueLines(tokensWithData)
    }
  }
}

object CommandToEncoding {
  private val ZERO       = Buf.Utf8("0")

  private val OPEN       = Buf.Utf8("/open")
  private val CLOSE      = Buf.Utf8("/close")
  private val CLOSE_OPEN = Buf.Utf8("/close/open")
  private val ABORT      = Buf.Utf8("/abort")
  private val PEEK       = Buf.Utf8("/peek")
  private val TIMEOUT    = Buf.Utf8("/t=")

  private val GET        = Buf.Utf8("get")
  private val DELETE     = Buf.Utf8("delete")
  private val FLUSH      = Buf.Utf8("flush")

  private val SET        = Buf.Utf8("set")


}

private[kestrel] class CommandToEncoding extends OneToOneEncoder {
  import CommandToEncoding._

  // kestrel supports only 32-bit timeouts
  private[this] def encodeTimeout(timeout: Duration): Buf = {
    val timeInMillis = math.min(timeout.inMilliseconds, Int.MaxValue)
    TIMEOUT.concat(Buf.Utf8(timeInMillis.toString))
  }

  private[this] def keyWithSuffix(
    queueName: Buf,
    suffix: Buf,
    timeout: Option[Duration]
  ): Buf = {
    timeout match {
      case Some(t) => queueName.concat(suffix).concat(encodeTimeout(t))
      case None    => queueName.concat(suffix)
    }
  }

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = {
    message match {
      case Set(key, expiry, value) =>
        TokensWithData(Seq(SET, key, ZERO, Buf.Utf8(expiry.inSeconds.toString)), value)
      case Get(queueName, timeout) =>
        val key = timeout match {
          case Some(t) => queueName.concat(encodeTimeout(t))
          case None => queueName
        }
        Tokens(Seq(GET, key))
      case Open(queueName, timeout) =>
        val key = keyWithSuffix(queueName, OPEN, timeout)
        Tokens(Seq(GET, key))
      case Close(queueName, timeout) =>
        val key = keyWithSuffix(queueName, CLOSE, timeout)
        Tokens(Seq(GET, key))
      case CloseAndOpen(queueName, timeout) =>
        val key = keyWithSuffix(queueName, CLOSE_OPEN, timeout)
        Tokens(Seq(GET, key))
      case Abort(queueName, timeout) =>
        val key = keyWithSuffix(queueName, ABORT, timeout)
        Tokens(Seq(GET, key))
      case Peek(queueName, timeout) =>
        val key = keyWithSuffix(queueName, PEEK, timeout)
        Tokens(Seq(GET, key))
      case Delete(key) =>
        Tokens(Seq(DELETE, key))
      case Flush(key) =>
        Tokens(Seq(FLUSH, key))
    }
  }
}
