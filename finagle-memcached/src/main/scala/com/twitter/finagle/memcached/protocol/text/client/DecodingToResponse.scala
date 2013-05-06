package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import java.nio.charset.{Charset => JCharset}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder
import text.{StatLines, TokensWithData, ValueLines, Tokens}

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
    case StatLines(lines) =>
      parseStatLines(lines)
    case _ => throw new IllegalArgumentException("Expecting a Decoding")
  }

  protected def parseResponse(tokens: Seq[ChannelBuffer]): R
  protected def parseValues(valueLines: Seq[TokensWithData]): R
  protected def parseStatLines(lines: Seq[Tokens]): R
}

class DecodingToResponse extends AbstractDecodingToResponse[Response] {
  import AbstractDecodingToResponse._
  private[this] val asciiCharset = JCharset.forName("US-ASCII")

  protected def parseResponse(tokens: Seq[ChannelBuffer]) = {
    tokens.headOption match {
      case None               => NoOp()
      case Some(NOT_FOUND)    => NotFound()
      case Some(STORED)       => Stored()
      case Some(NOT_STORED)   => NotStored()
      case Some(EXISTS)       => Exists()
      case Some(DELETED)      => Deleted()
      case Some(ERROR)        => Error(new NonexistentCommand(parseErrorMessage(tokens)))
      case Some(CLIENT_ERROR) => Error(new ClientError(parseErrorMessage(tokens)))
      case Some(SERVER_ERROR) => Error(new ServerError(parseErrorMessage(tokens)))
      case Some(ds)           => Number(ds.toLong)
    }
  }

  protected def parseStatLines(lines: Seq[Tokens]) = {
    val l = lines.map { line =>
      val tokens = line.tokens
      Info(tokens(0), tokens.drop(1))
    }
    InfoLines(l)
  }

  protected def parseValues(valueLines: Seq[TokensWithData]) = {
    val values = valueLines.map { valueLine =>
      val tokens = valueLine.tokens
      val flag = if (tokens.length >= 3) Some(tokens(2)) else None
      val casUnique = if (tokens.length == 5) Some(tokens(4)) else None
      Value(tokens(1), valueLine.data, casUnique, flag)
    }
    Values(values)
  }

  private[this] def parseErrorMessage(tokens: Seq[ChannelBuffer]) = {
    tokens.lastOption map { _.toString(asciiCharset) } getOrElse("")
  }
}
