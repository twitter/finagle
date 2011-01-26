package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}

object AbstractDecodingToResponse {
  private[finagle] val STORED     = "STORED": ChannelBuffer
  private[finagle] val NOT_FOUND  = "NOT_FOUND": ChannelBuffer
  private[finagle] val NOT_STORED = "NOT_STORED": ChannelBuffer
  private[finagle] val DELETED    = "DELETED": ChannelBuffer
}

abstract class AbstractDecodingToResponse[R <: AnyRef] extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, ch: Channel, m: AnyRef): R = m match {
    case Tokens(tokens) =>
      parseResponse(tokens)
    case ValueLines(lines) =>
      parseValues(lines)
    case _ => throw new IllegalArgumentException("Expecting a Decoding")
  }

  protected def parseResponse(tokens: Seq[ChannelBuffer]): R
  protected def parseValues(valueLines: Seq[TokensWithData]): R
}

class DecodingToResponse extends AbstractDecodingToResponse[Response] {
  import AbstractDecodingToResponse._

  protected def parseResponse(tokens: Seq[ChannelBuffer]) = {
    tokens.head match {
      case NOT_FOUND  => NotFound()
      case STORED     => Stored()
      case NOT_STORED => NotStored()
      case DELETED    => Deleted()
      case ds         => Number(ds.toInt)
    }
  }

  protected def parseValues(valueLines: Seq[TokensWithData]) = {
    val values = valueLines.map { valueLine =>
      val tokens = valueLine.tokens
//      val cas = if (tokens.length == 4) Some(tokens(4).toInt) else None
      Value(tokens(1), valueLine.data)
    }
    Values(values)
  }
}