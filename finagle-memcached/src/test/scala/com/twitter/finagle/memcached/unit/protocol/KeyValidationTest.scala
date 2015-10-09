package com.twitter.finagle.memcached.unit.protocol

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.memcached.protocol.KeyValidation
import com.twitter.io.{Buf, Charsets}

@RunWith(classOf[JUnitRunner])
class KeyValidationTest extends FunSuite {

  private class BasicKeyValidation(
    override val keys: Seq[Buf]
  ) extends KeyValidation

  test("reject invalid key that is too long") {
    val length = 251
    val key = "x" * length

    val x = intercept[IllegalArgumentException] {
      new BasicKeyValidation(Seq(Buf.Utf8(key)))
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
    ) map { Buf.Utf8(_) }

    bads foreach { bad =>
      val x = intercept[IllegalArgumentException] {
        new BasicKeyValidation(Seq(bad))
      }
      assert(x.getMessage.contains("key cannot have whitespace or control characters"))
    }
  }
}
