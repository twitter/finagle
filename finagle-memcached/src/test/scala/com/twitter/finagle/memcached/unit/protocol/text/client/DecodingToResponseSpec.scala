package com.twitter.finagle.memcached.unit.protocol.text.client

import org.specs.Specification
import com.twitter.finagle.memcached.protocol.text.client.DecodingToResponse
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.memcached.protocol.text.Tokens
import com.twitter.finagle.memcached.protocol
import com.twitter.finagle.memcached.protocol.{ClientError, Stored, NonexistentCommand, NotFound, Exists}

object DecodingToResponseSpec extends Specification {
  "DecodingToResponse" should {
    val decodingToResponse = new DecodingToResponse

    "parseResponse" in {
      "NOT_FOUND" in {
        val buffer = Tokens(Seq[ChannelBuffer]("NOT_FOUND"))
        decodingToResponse.decode(null, null, buffer) mustEqual NotFound()
      }

      "STORED" in {
        val buffer = Tokens(Seq[ChannelBuffer]("STORED"))
        decodingToResponse.decode(null, null, buffer) mustEqual Stored()
      }

      "EXISTS" in {
        val buffer = Tokens(Seq[ChannelBuffer]("EXISTS"))
        decodingToResponse.decode(null, null, buffer) mustEqual Exists()
      }

      "ERROR" in {
        val buffer = Tokens(Seq[ChannelBuffer]("ERROR"))
        decodingToResponse.decode(null, null, buffer).asInstanceOf[protocol.Error].cause must
          haveClass[NonexistentCommand]
      }

      "CLIENT_ERROR" in {
        val buffer = Tokens(Seq[ChannelBuffer]("CLIENT_ERROR"))
        decodingToResponse.decode(null, null, buffer).asInstanceOf[protocol.Error].cause must
          haveClass[ClientError]
      }
    }
  }
}
