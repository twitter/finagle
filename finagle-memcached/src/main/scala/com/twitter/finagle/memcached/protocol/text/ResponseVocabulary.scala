package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.text.client.ValueLine
import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.util.ParserUtils

object ResponseVocabulary {
  private[finagle] val VALUE      = "VALUE": ChannelBuffer
  private[finagle] val STORED     = "STORED": ChannelBuffer
  private[finagle] val NOT_FOUND  = "NOT_FOUND": ChannelBuffer
  private[finagle] val NOT_STORED = "NOT_STORED": ChannelBuffer
  private[finagle] val DELETED    = "DELETED": ChannelBuffer
}

trait ResponseVocabulary[R] {
  import ResponseVocabulary._
  import ParserUtils._

  def parseResponse(tokens: Seq[ChannelBuffer]): R
  def parseValues(valueLines: Seq[ValueLine]): R

  def needsData(tokens: Seq[ChannelBuffer]) = {
    val responseName = tokens.head
    val args = tokens.tail
    if (responseName == VALUE) {
      validateValueResponse(args)
      Some(args(2).toInt)
    } else None
  }

  private[this] def validateValueResponse(args: Seq[ChannelBuffer]) {
    if (args.length < 3) throw new ServerError("Too few arguments")
    if (args.length > 4) throw new ServerError("Too many arguments")
    if (args.length == 4 && !args(3).matches(DIGITS)) throw new ServerError("CAS must be a number")
    if (!args(2).matches(DIGITS)) throw new ServerError("Bytes must be number")
  }
}

class MemcachedResponseVocabulary extends ResponseVocabulary[Response] {
  import ResponseVocabulary._

  def parseResponse(tokens: Seq[ChannelBuffer]) = {
    tokens.head match {
      case NOT_FOUND  => NotFound()
      case STORED     => Stored()
      case NOT_STORED => NotStored()
      case DELETED    => Deleted()
      case ds         => Number(ds.toInt)
    }
  }

  def parseValues(valueLines: Seq[ValueLine]) = {
    val values = valueLines.map { valueLine =>
      val tokens = valueLine.tokens
//      val cas = if (tokens.length == 4) Some(tokens(4).toInt) else None
      Value(tokens(1), valueLine.buffer)
    }
    Values(values)
  }
}