package com.twitter.finagle.memcached.unit.protocol.text.server

import org.specs.Specification
import org.specs.util.DataTables
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer 
import org.jboss.netty.util.CharsetUtil
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.protocol.text.TokensWithData
import com.twitter.finagle.memcached.protocol.text.server.DecodingToCommand
import com.twitter.finagle.memcached.protocol.Set
import com.twitter.util.{Duration, Time}

object DecodingToCommandSpec extends Specification with DataTables {
  "DecodingToCommand" should {
    val decodingToCommand = new DecodingToCommand

    "parseCommand" in {
      "SET" in {
        val key = "testKey"
        val flags = "0"
        val data = "Hello World"
        val dataSize = data.size.toString

        "with expire times" in {
          "expire time"                 | "expected value"    | "allowed delta" |>
          0                             ! Time.epoch          ! 0.seconds       |
          200.seconds.fromNow.inSeconds ! 200.seconds.fromNow ! 1.seconds       |
          200                           ! 200.seconds.fromNow ! 1.seconds       |
          { (exptime: Int, expectedExpiration: Time, delta: Duration) =>
            val buffer = TokensWithData(Seq[ChannelBuffer]("set", key, flags, exptime.toString, dataSize), data, None)
            val command = decodingToCommand.decode(null, null, buffer)
            command must haveClass[Set]
            val set = command.asInstanceOf[Set]
            set.key.toString(CharsetUtil.UTF_8) mustEqual key
            set.value.toString(CharsetUtil.UTF_8) mustEqual data
            set.expiry.moreOrLessEquals(expectedExpiration, delta) must beTrue
          }
        }
      }

    }

  }
}
