package com.twitter.finagle.memcached.unit.protocol.text.server

import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.server.DecodingToCommand
import com.twitter.finagle.memcached.protocol.text.{Tokens, TokensWithData}
import com.twitter.finagle.memcached.protocol.{Set, Stats}
import com.twitter.io.Buf
import com.twitter.util.{Duration, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.TableDrivenPropertyChecks._

@RunWith(classOf[JUnitRunner])
class DecodingToCommandTest extends FunSuite {

  class Context {
    val decodingToCommand = new DecodingToCommand
    case class ExpectedTimeTable(expireTime: Int, expirationTime: Time)
  }

  test("parseCommand SET with expire times") {
    val context = new Context
    import context._

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
        val buffer = TokensWithData(
          Seq("set", key, flags, expectedTime.expireTime.toString, dataSize).map(Buf.Utf8(_)),
          Buf.Utf8(data),
          None
        )
        val command = decodingToCommand.decode(null, null, buffer)
        assert(command.getClass == classOf[Set])
        val set = command.asInstanceOf[Set]
        assert(set.key == Buf.Utf8(key))
        assert(set.value == Buf.Utf8(data))
        assert(set.expiry.moreOrLessEquals(expectedTime.expirationTime, allowedDelta))
      }
    }
  }

  test("parseCommand STAT") {
    val context = new Context
    import context._

    Seq(None, Some("slabs"), Some("items")).foreach { arg =>
      val cmd = arg match {
        case None => Seq("stats") map { Buf.Utf8(_) }
        case Some(s) => Seq("stats", s) map { Buf.Utf8(_) }
      }
      val buffer = Tokens(cmd)
      val command = decodingToCommand.decode(null, null, buffer)
      assert(command.getClass == classOf[Stats])
      val stats = command.asInstanceOf[Stats]
      stats.args.headOption match {
        case None => assert(arg == None)
        case Some(Buf.Utf8(cb)) => assert(cb == arg.get)
      }
    }
  }

}
