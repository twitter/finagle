package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.memcached.protocol.text._
import com.twitter.io.Buf
import com.twitter.util.Duration

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

private[finagle] class CommandToEncoding extends AbstractCommandToEncoding[Command] {
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

  def encode(message: Command): Decoding = {
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
