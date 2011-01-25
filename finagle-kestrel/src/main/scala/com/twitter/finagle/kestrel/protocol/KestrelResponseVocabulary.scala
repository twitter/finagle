package com.twitter.finagle.kestrel.protocol

import com.twitter.finagle.memcached.protocol.text.client.ValueLine
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.protocol.text.ResponseVocabulary

class KestrelResponseVocabulary extends ResponseVocabulary[Response] {
  import ResponseVocabulary._

  def parseResponse(tokens: Seq[ChannelBuffer]) = {
    tokens.head match {
      case NOT_FOUND  => NotFound()
      case STORED     => Stored()
      case DELETED    => Deleted()
    }
  }

  def parseValues(valueLines: Seq[ValueLine]) = {
    val values = valueLines.map { valueLine =>
      val tokens = valueLine.tokens
      Value(tokens(1), valueLine.buffer)
    }
    Values(values)
  }
}