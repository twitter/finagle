package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.memcached.protocol.text.{Tokens, TokensWithData}
import com.twitter.finagle.memcached.protocol.text.client.AbstractDecodingToResponse
import com.twitter.io.Buf

private[kestrel] class DecodingToResponse extends AbstractDecodingToResponse[Response] {
  import AbstractDecodingToResponse._

  def parseResponse(tokens: Seq[Buf]) = {
    tokens.head match {
      case NOT_FOUND  => NotFound()
      case STORED     => Stored()
      case DELETED    => Deleted()
      case ERROR      => Error()
      case _          => Error()
    }
  }

  def parseValues(valueLines: Seq[TokensWithData]) = {
    val values = valueLines.map { valueLine =>
      val tokens = valueLine.tokens
      Value(tokens(1), valueLine.data)
    }
    Values(values)
  }

  def parseStatLines(lines: Seq[Tokens]) = Error()
}
