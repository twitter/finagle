package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.{StatLines, TokensWithData, ValueLines, Tokens}
import com.twitter.io.Buf
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder

object AbstractDecodingToResponse {
  private[finagle] val STORED        = Buf.Utf8("STORED")
  private[finagle] val NOT_FOUND     = Buf.Utf8("NOT_FOUND")
  private[finagle] val NOT_STORED    = Buf.Utf8("NOT_STORED")
  private[finagle] val EXISTS        = Buf.Utf8("EXISTS")
  private[finagle] val DELETED       = Buf.Utf8("DELETED")
  private[finagle] val ERROR         = Buf.Utf8("ERROR")
  private[finagle] val CLIENT_ERROR  = Buf.Utf8("CLIENT_ERROR")
  private[finagle] val SERVER_ERROR  = Buf.Utf8("SERVER_ERROR")
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

  protected def parseResponse(tokens: Seq[Buf]): R
  protected def parseValues(valueLines: Seq[TokensWithData]): R
  protected def parseStatLines(lines: Seq[Tokens]): R
}

class DecodingToResponse extends AbstractDecodingToResponse[Response] {
  import AbstractDecodingToResponse._
  import com.twitter.finagle.memcached.util.Bufs.RichBuf

  protected def parseResponse(tokens: Seq[Buf]) = {
    tokens.headOption match {
      case None               => Response.NoOp
      case Some(NOT_FOUND)    => Response.NotFound
      case Some(STORED)       => Response.Stored
      case Some(NOT_STORED)   => Response.NotStored
      case Some(EXISTS)       => Response.Exists
      case Some(DELETED)      => Response.Deleted
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

  private[this] def parseErrorMessage(tokens: Seq[Buf]) =
    tokens.drop(1).map { case Buf.Utf8(s) => s }.mkString(" ")

}
