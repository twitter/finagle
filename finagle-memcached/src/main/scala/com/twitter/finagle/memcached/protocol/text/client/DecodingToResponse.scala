package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import text.{TokensWithData, ValueLines, Tokens}

object AbstractDecodingToResponse {
  private[finagle] val STORED        = "STORED":          ChannelBuffer
  private[finagle] val NOT_FOUND     = "NOT_FOUND":       ChannelBuffer
  private[finagle] val NOT_STORED    = "NOT_STORED":      ChannelBuffer
  private[finagle] val EXISTS        = "EXISTS":          ChannelBuffer
  private[finagle] val DELETED       = "DELETED":         ChannelBuffer
  private[finagle] val ERROR         = "ERROR":           ChannelBuffer
  private[finagle] val CLIENT_ERROR  = "CLIENT_ERROR":    ChannelBuffer
  private[finagle] val SERVER_ERROR  = "SERVER_ERROR":    ChannelBuffer
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
    tokens.headOption match {
      case None               => NoOp()
      case Some(NOT_FOUND)    => NotFound()
      case Some(STORED)       => Stored()
      case Some(NOT_STORED)   => NotStored()
      case Some(EXISTS)       => Exists()
      case Some(DELETED)      => Deleted()
      case Some(ERROR)        => Error(new NonexistentCommand(""))
      case Some(CLIENT_ERROR) => Error(new ClientError(""))
      case Some(SERVER_ERROR) => Error(new ServerError(""))
      case Some(ds)           => Number(ds.toLong)
    }
  }

  protected def parseValues(valueLines: Seq[TokensWithData]) = {
    val values = valueLines.map { valueLine =>
      val tokens = valueLine.tokens
      val casUnique = if (tokens.length == 5) Some(tokens(4)) else None
      Value(tokens(1), valueLine.data, casUnique)
    }
    Values(values)
  }
}
