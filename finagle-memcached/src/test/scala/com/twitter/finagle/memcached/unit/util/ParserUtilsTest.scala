package com.twitter.finagle.memcached.unit.util

import com.google.common.base.Charsets
import com.twitter.finagle.memcached.util.ParserUtils
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParserUtilsTest extends FunSuite {

  private def isDigits(str: String): Boolean = {
    val cb = ChannelBuffers.copiedBuffer(str, Charsets.UTF_8)
    ParserUtils.isDigits(cb)
  }

  test("isDigits") {
    assert(isDigits("123"))
    assert(isDigits("1"))
    assert(!isDigits(""))
    assert(!isDigits(" "))
    assert(!isDigits("x"))
    assert(!isDigits(" 9"))
    assert(!isDigits("9 "))
  }
}
