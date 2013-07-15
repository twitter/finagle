package com.twitter.finagle.memcached.unit.protocol.text.server

import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.server.DecodingToCommand
import com.twitter.finagle.memcached.protocol.text.{Tokens, TokensWithData}
import com.twitter.finagle.memcached.protocol.{Set, Stats}
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.{Duration, Time}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit
import org.specs.util.DataTables

class DecodingToCommandSpec extends SpecificationWithJUnit with DataTables {
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

      "STAT" in {
        Seq(None, Some("slabs"), Some("items")).foreach { arg =>
          val cmd: Seq[ChannelBuffer] = arg match {
            case None => Seq("stats")
            case Some(s) => Seq("stats", s)
          }
          val buffer = Tokens(cmd)
          val command = decodingToCommand.decode(null, null, buffer)
          command must haveClass[Stats]
          val stats = command.asInstanceOf[Stats]
          stats.args.headOption match {
            case None => arg must beNone
            case Some(cb) => cb.toString(CharsetUtil.UTF_8) mustEqual arg.get
          }
        }
      }

    }

  }
}
