package com.twitter.finagle.memcached.unit.protocol

import com.twitter.finagle.memcached.protocol.KeyValidation
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.util.CharsetUtil
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.MustMatchers

@RunWith(classOf[JUnitRunner])
class KeyValidationTest extends FunSuite
  with MustMatchers
{

  private class BasicKeyValidation(
    override val keys: Seq[ChannelBuffer]
  ) extends KeyValidation

  test("reject invalid key that is too long") {
    val length = 251
    val key = "x" * length
    val cb = ChannelBuffers.copiedBuffer(key, CharsetUtil.UTF_8)

    val x = intercept[IllegalArgumentException] {
      new BasicKeyValidation(Seq(cb))
    }
    x.getMessage must include("key cannot be longer than")
    x.getMessage must include("(" + length + ")")
  }

  test("reject invalid key with whitespace or control chars") {
    val bads = Seq(
      "hi withwhitespace",
      "anda\rcarraigereturn",
      "yo\u0000ihaveacontrolchar",
      "andheres\nanewline"
    ) map { ChannelBuffers.copiedBuffer(_, CharsetUtil.UTF_8) }

    bads foreach { bad =>
      val x = intercept[IllegalArgumentException] {
        new BasicKeyValidation(Seq(bad))
      }
      x.getMessage must include("key cannot have whitespace or control characters")
    }
  }

}
