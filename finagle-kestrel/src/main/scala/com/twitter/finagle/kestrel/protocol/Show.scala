package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import com.twitter.finagle.memcached.protocol.text.{Decoding, Tokens, TokensWithData, ValueLines}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.Duration

object ResponseToEncoding {
  private val ZERO          = "0"
  private val VALUE         = "VALUE"
  private val ZeroCb: ChannelBuffer  = ChannelBuffers.unmodifiableBuffer(ZERO)
  private val ValueCb: ChannelBuffer = ChannelBuffers.unmodifiableBuffer(VALUE)

  private val STORED        = "STORED"
  private val NOT_FOUND     = "NOT_FOUND"
  private val DELETED       = "DELETED"
  private val ERROR         = "ERROR"

  private val StoredTokens   = Tokens(Seq(ChannelBuffers.unmodifiableBuffer(STORED)))
  private val NotFoundTokens = Tokens(Seq(ChannelBuffers.unmodifiableBuffer(NOT_FOUND)))
  private val DeletedTokens  = Tokens(Seq(ChannelBuffers.unmodifiableBuffer(DELETED)))
  private val ErrorTokens    = Tokens(Seq(ChannelBuffers.unmodifiableBuffer(ERROR)))
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
  private val ZERO: ChannelBuffer = ChannelBuffers.unmodifiableBuffer("0")

  private val OPEN: ChannelBuffer  = ChannelBuffers.unmodifiableBuffer("/open")
  private val CLOSE: ChannelBuffer = ChannelBuffers.unmodifiableBuffer("/close")
  private val CLOSE_OPEN: ChannelBuffer = ChannelBuffers.unmodifiableBuffer("/close/open")
  private val ABORT: ChannelBuffer = ChannelBuffers.unmodifiableBuffer("/abort")
  private val PEEK: ChannelBuffer  = ChannelBuffers.unmodifiableBuffer("/peek")
  private val TIMEOUT: ChannelBuffer = ChannelBuffers.unmodifiableBuffer("/t=")

  private val GET: ChannelBuffer    = ChannelBuffers.unmodifiableBuffer("get")
  private val DELETE: ChannelBuffer = ChannelBuffers.unmodifiableBuffer("delete")
  private val FLUSH: ChannelBuffer  = ChannelBuffers.unmodifiableBuffer("flush")

  private val SET: ChannelBuffer    = ChannelBuffers.unmodifiableBuffer("set")


}

private[kestrel] class CommandToEncoding extends OneToOneEncoder {
  import CommandToEncoding._

  // kestrel supports only 32-bit timeouts
  private[this] def encodeTimeout(timeout: Duration): ChannelBuffer = {
    val timeInMillis = math.min(timeout.inMilliseconds, Int.MaxValue)
    ChannelBuffers.wrappedBuffer(TIMEOUT, timeInMillis.toString)
  }

  private[this] def keyWithSuffix(
    queueName: ChannelBuffer,
    suffix: ChannelBuffer,
    timeout: Option[Duration]
  ): ChannelBuffer = {
    timeout match {
      case Some(t) => ChannelBuffers.wrappedBuffer(queueName, suffix, encodeTimeout(t))
      case None => ChannelBuffers.wrappedBuffer(queueName, suffix)
    }
  }

  def encode(ctx: ChannelHandlerContext, ch: Channel, message: AnyRef): Decoding = {
    message match {
      case Set(key, expiry, value) =>
        TokensWithData(Seq(SET, key, ZERO, expiry.inSeconds.toString), value)
      case Get(queueName, timeout) =>
        val key = timeout match {
          case Some(t) => ChannelBuffers.wrappedBuffer(queueName, encodeTimeout(t))
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
