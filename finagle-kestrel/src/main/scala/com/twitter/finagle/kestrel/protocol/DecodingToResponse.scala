package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.protocol.text.TokensWithData
import com.twitter.finagle.memcached.protocol.text.client.AbstractDecodingToResponse

class DecodingToResponse extends AbstractDecodingToResponse[Response] {
  import AbstractDecodingToResponse._

  def parseResponse(tokens: Seq[ChannelBuffer]) = {
    tokens.head match {
      case NOT_FOUND  => NotFound()
      case STORED     => Stored()
      case DELETED    => Deleted()
    }
  }

  def parseValues(valueLines: Seq[TokensWithData]) = {
    val values = valueLines.map { valueLine =>
      val tokens = valueLine.tokens
      Value(tokens(1), valueLine.data)
    }
    Values(values)
  }
}