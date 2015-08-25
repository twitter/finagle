package com.twitter.finagle.memcached.unit.util

import com.twitter.finagle.memcached.util.ChannelBufferUtils
import com.twitter.io.Charsets
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class RichChannelBufferTest extends FunSuite
  with GeneratorDrivenPropertyChecks
{

  test("toInt for 0 to Int.MaxValue") {
    import ChannelBufferUtils.{stringToChannelBuffer, channelBufferToRichChannelBuffer}

    forAll(Gen.chooseNum(0, Int.MaxValue)) { n: Int =>
      val cb: ChannelBuffer = n.toString
      assert(n == cb.toInt)
    }
  }

  test("toInt for bad input") {
    import ChannelBufferUtils.channelBufferToRichChannelBuffer

    Seq("", "abc", "123Four", "-1", "1" * 11, "2147483648")
      .map(ChannelBuffers.copiedBuffer(_, Charsets.Utf8))
      .foreach { cb =>
        intercept[NumberFormatException] {
          cb.toInt
        }
      }
  }

}
