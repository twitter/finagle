package com.twitter.finagle.memcached.unit.protocol.text.server

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached.protocol.StorageCommand.StorageCommands
import com.twitter.finagle.memcached.protocol.text.server.MemcachedServerDecoder
import com.twitter.finagle.memcached.protocol.{Set, Stats}
import com.twitter.io.Buf
import com.twitter.util.{Duration, Time}
import org.scalatest.OneInstancePerTest
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.funsuite.AnyFunSuite

class MemcachedServerDecoderTest extends AnyFunSuite with OneInstancePerTest {

  val decoder = new MemcachedServerDecoder(StorageCommands)
  case class ExpectedTimeTable(expireTime: Int, expirationTime: Time)

  test("parseCommand SET with expire times") {
    val key = "testKey"
    val flags = "0"
    val data = "Hello World"
    val dataSize = data.length.toString

    Time.withCurrentTimeFrozen { _ =>
      val expireTimeTableData =
        Table(
          "expectedTime" -> "allowedDelta",
          ExpectedTimeTable(0, Time.epoch) -> 0.seconds,
          ExpectedTimeTable(200.seconds.fromNow.inSeconds, 200.seconds.fromNow) -> 1.seconds,
          ExpectedTimeTable(200, 200.seconds.fromNow) -> 1.seconds
        )

      forAll(expireTimeTableData) { (expectedTime: ExpectedTimeTable, allowedDelta: Duration) =>
        // first frame; decoding should return null because expecting data frame to follow
        assert(
          decoder.decode(Buf.Utf8(s"set $key $flags ${expectedTime.expireTime} $dataSize")) == null
        )
        val command = decoder.decode(Buf.Utf8(data))
        assert(command.getClass == classOf[Set])
        val set = command.asInstanceOf[Set]
        assert(set.key == Buf.Utf8(key))
        assert(set.value == Buf.Utf8(data))
        assert(set.expiry.moreOrLessEquals(expectedTime.expirationTime, allowedDelta))
      }
    }
  }

  test("parseCommand STAT") {
    Seq(None, Some("slabs"), Some("items")).foreach { arg =>
      val buffer = arg match {
        case None => Buf.Utf8("stats")
        case Some(s) => Buf.Utf8(s"stats $s")
      }
      val command = decoder.decode(buffer)
      assert(command.getClass == classOf[Stats])
      val stats = command.asInstanceOf[Stats]
      stats.args.headOption match {
        case None => assert(arg == None)
        case Some(Buf.Utf8(cb)) => assert(cb == arg.get)
      }
    }
  }
}
