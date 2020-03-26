package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol.{Value => MValue, _}
import com.twitter.io.Buf

private[finagle] final class MemcachedClientDecoder extends ClientDecoder[Response] {
  import MemcachedClientDecoder._
  import com.twitter.finagle.memcached.util.Bufs.RichBuf

  protected type Value = MValue

  protected def parseResponse(tokens: Seq[Buf]): Response = {
    if (tokens.isEmpty) NoOp
    else
      tokens.head match {
        case NOT_FOUND => NotFound
        case STORED => Stored
        case NOT_STORED => NotStored
        case EXISTS => Exists
        case DELETED => Deleted
        case ERROR => Error(new NonexistentCommand(parseErrorMessage(tokens)))
        case CLIENT_ERROR => Error(new ClientError(parseErrorMessage(tokens)))
        case SERVER_ERROR => Error(new ServerError(parseErrorMessage(tokens)))
        case ds => Number(ds.toLong)
      }
  }

  protected def parseStatLines(lines: Seq[Tokens]): Response = {
    val l = lines.map { tokens => Info(tokens(0), tokens.drop(1)) }
    InfoLines(l)
  }

  protected def parseValue(tokens: Seq[Buf], data: Buf): Value = {
    val tokensLength = tokens.length
    if (tokensLength < 2) {
      throw new ServerError(s"Received value line with insufficient number of tokens: $tokens")
    }

    val flag = if (tokensLength >= 3) Some(tokens(2)) else None
    val casUnique = if (tokensLength == 5) Some(tokens(4)) else None
    MValue(tokens(1), data, casUnique, flag)
  }

  protected def parseResponseValues(values: Seq[Value]): Response = Values(values)

  private[this] def parseErrorMessage(tokens: Seq[Buf]): String =
    tokens.drop(1).map { case Buf.Utf8(s) => s }.mkString(" ")

}

private[finagle] object MemcachedClientDecoder {
  private[finagle] val STORED = Buf.Utf8("STORED")
  private[finagle] val NOT_FOUND = Buf.Utf8("NOT_FOUND")
  private[finagle] val NOT_STORED = Buf.Utf8("NOT_STORED")
  private[finagle] val EXISTS = Buf.Utf8("EXISTS")
  private[finagle] val DELETED = Buf.Utf8("DELETED")
  private[finagle] val ERROR = Buf.Utf8("ERROR")
  private[finagle] val CLIENT_ERROR = Buf.Utf8("CLIENT_ERROR")
  private[finagle] val SERVER_ERROR = Buf.Utf8("SERVER_ERROR")
}
