package com.twitter.finagle.memcached.protocol

import text.client.ValueLine
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.util.ParserUtils

trait ResponseParser[R, V] {
  def parseResponse(tokens: Seq[ChannelBuffer]): R
  def parseValues(valueLines: Seq[ValueLine]): V
  def needsData(tokens: Seq[ChannelBuffer]): Option[Int]
}

object ParseResponse {
  private val VALUE      = "VALUE": ChannelBuffer
  private val STORED     = "STORED": ChannelBuffer
  private val NOT_FOUND  = "NOT_FOUND": ChannelBuffer
  private val NOT_STORED = "NOT_STORED": ChannelBuffer
  private val DELETED    = "DELETED": ChannelBuffer
}

class ParseResponse extends ResponseParser[Response, Values] {
  import ParseResponse._
  import ParserUtils._

  def parseResponse(tokens: Seq[ChannelBuffer]) = {
    tokens.head match {
      case NOT_FOUND  => NotFound
      case STORED     => Stored
      case NOT_STORED => NotStored
      case DELETED    => Deleted
      case ds         => Number(ds.toInt)
    }
  }

  def parseValues(valueLines: Seq[ValueLine]) = {
    val values = valueLines.map { valueLine =>
      val tokens = valueLine.tokens
      val cas = if (tokens.length == 4) Some(tokens(4).toInt) else None
      Value(tokens(1), valueLine.buffer)
    }
    Values(values)
  }


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