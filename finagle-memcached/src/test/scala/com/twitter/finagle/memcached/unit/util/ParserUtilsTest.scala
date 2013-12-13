package com.twitter.finagle.memcached.unit.util

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.memcached.util.ParserUtils
import org.jboss.netty.buffer.ChannelBuffers
import com.google.common.base.Charsets

@RunWith(classOf[JUnitRunner])
class ParserUtilsTest extends FunSuite
  with ShouldMatchers
{

  private def isDigits(str: String): Boolean = {
    val cb = ChannelBuffers.copiedBuffer(str, Charsets.UTF_8)
    ParserUtils.isDigits(cb)
  }

  test("isDigits") {
    isDigits("123") should be (true)
    isDigits("1") should be (true)

    isDigits("") should be (false)
    isDigits(" ") should be (false)
    isDigits("x") should be (false)
    isDigits(" 9") should be (false)
    isDigits("9 ") should be (false)
  }

}
