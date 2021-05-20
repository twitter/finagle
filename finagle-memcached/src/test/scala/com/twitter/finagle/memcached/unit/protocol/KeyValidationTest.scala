package com.twitter.finagle.memcached.unit.protocol

import com.twitter.finagle.memcached.protocol.KeyValidation
import com.twitter.io.Buf
import org.scalatest.funsuite.AnyFunSuite

class KeyValidationTest extends AnyFunSuite {

  private val illegalCharKeys = Seq(
    "hi withwhitespace",
    "anda\rcarraigereturn",
    "yo\u0000ihaveacontrolchar",
    "andheres\nanewline"
  ).map { Buf.Utf8(_) }

  private val tooLongKey = Buf.Utf8("x" * 251)

  test("reject a null key") {
    val x = intercept[IllegalArgumentException] {
      KeyValidation.checkKey(null)
    }
    assert(x.getMessage == "Invalid keys: key cannot be null")
  }

  test("reject invalid key that is too long") {
    val x = intercept[IllegalArgumentException] {
      KeyValidation.checkKey(tooLongKey)
    }
    assert(
      x.getMessage == s"Invalid keys: key cannot be longer than 250 bytes (${tooLongKey.length})"
    )
  }

  test("reject invalid key with whitespace or control chars") {
    illegalCharKeys.foreach { bad =>
      val x = intercept[IllegalArgumentException] {
        KeyValidation.checkKey(bad)
      }
      assert(x.getMessage.contains("key cannot have whitespace or control characters: '0x"))
    }
  }

  test("reject a null collection of keys") {
    val x = intercept[IllegalArgumentException] {
      KeyValidation.checkKeys(null)
    }
    assert(x.getMessage == "Invalid keys: cannot have null for keys")
  }

  test("reject a collection containing null") {
    val x = intercept[IllegalArgumentException] {
      KeyValidation.checkKeys(Seq(null))
    }
    assert(x.getMessage == "Invalid keys: key cannot be null")
  }

  test("reject a collection containing an invalid key") {
    (illegalCharKeys :+ tooLongKey).foreach { bad =>
      intercept[IllegalArgumentException] {
        KeyValidation.checkKeys(Seq(bad))
      }
    }
  }
}
