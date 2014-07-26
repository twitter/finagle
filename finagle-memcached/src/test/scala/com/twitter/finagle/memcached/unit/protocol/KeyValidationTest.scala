package com.twitter.finagle.memcached.unit.protocol

import com.twitter.finagle.memcached.protocol.KeyValidation
import com.twitter.io.Charsets
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KeyValidationTest extends FunSuite {

  private class BasicKeyValidation(
    override val keys: Seq[ChannelBuffer]
  ) extends KeyValidation

  test("reject invalid key that is too long") {
    val length = 251
    val key = "x" * length
    val cb = ChannelBuffers.copiedBuffer(key, Charsets.Utf8)

    val x = intercept[IllegalArgumentException] {
      new BasicKeyValidation(Seq(cb))
    }
    assert(x.getMessage.contains("key cannot be longer than"))
    assert(x.getMessage.contains("(" + length + ")"))
  }

  test("reject invalid key with whitespace or control chars") {
    val bads = Seq(
      "hi withwhitespace",
      "anda\rcarraigereturn",
      "yo\u0000ihaveacontrolchar",
      "andheres\nanewline"
    ) map { ChannelBuffers.copiedBuffer(_, Charsets.Utf8) }

    bads foreach { bad =>
      val x = intercept[IllegalArgumentException] {
        new BasicKeyValidation(Seq(bad))
      }
      assert(x.getMessage.contains("key cannot have whitespace or control characters"))
    }
  }

}
