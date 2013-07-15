package com.twitter.finagle.memcached.unit.protocol.text.client

import com.twitter.finagle.memcached.protocol
import com.twitter.finagle.memcached.protocol.text.client.DecodingToResponse
import com.twitter.finagle.memcached.protocol.text.{Tokens, StatLines}
import com.twitter.finagle.memcached.protocol.{ClientError, Info => MCInfo, InfoLines, Stored, NonexistentCommand, NotFound, Exists}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.util.CharsetUtil.UTF_8
import org.specs.SpecificationWithJUnit

class DecodingToResponseSpec extends SpecificationWithJUnit {
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

      "STATS" in {
        val lines = Seq(
          Seq("STAT", "items:1:number", "1"),
          Seq("STAT", "items:1:age", "1468"),
          Seq("ITEM", "foo", "[5 b;", "1322514067", "s]"))
        val plines = lines.map { line =>
          Tokens(line)
        }
        val info = decodingToResponse.decode(null, null, StatLines(plines))
        info must haveClass[InfoLines]
        val ilines = info.asInstanceOf[InfoLines].lines
        ilines must haveSize(lines.size)
        ilines.zipWithIndex.foreach { case(line, idx) =>
          val key = lines(idx)(0)
          val values = lines(idx).drop(1)
          line.key.toString(UTF_8) mustEqual key
          line.values.size mustEqual values.size
          line.values.zipWithIndex.foreach { case(token, tokIdx) =>
            token.toString(UTF_8) mustEqual values(tokIdx)
          }
        }
      }

      "CLIENT_ERROR" in {
        val errorMessage = "sad panda error"
        val buffer = Tokens(Seq[ChannelBuffer]("CLIENT_ERROR", errorMessage))
        val error = decodingToResponse.decode(null, null, buffer).asInstanceOf[protocol.Error]
        error.cause must haveClass[ClientError]
        error.cause.getMessage() mustEqual errorMessage
      }
    }
  }
}
