package com.twitter.finagle.ssl

import org.scalatest.funsuite.AnyFunSuite

class CipherSuitesTest extends AnyFunSuite {

  test("fromString results in unspecified when ciphers are empty") {
    val cipherSuites = CipherSuites.fromString("")

    assert(cipherSuites == CipherSuites.Unspecified)
  }

  test("fromString drops empty ciphers") {
    val cipherSuites1 = CipherSuites.fromString(":")
    assert(cipherSuites1 == CipherSuites.Unspecified, "cipherSuites1")

    val cipherSuites2 = CipherSuites.fromString("a:")
    assert(cipherSuites2 == CipherSuites.Enabled(Seq("a")), "cipherSuites2")

    val cipherSuites3 = CipherSuites.fromString(":b")
    assert(cipherSuites3 == CipherSuites.Enabled(Seq("b")), "cipherSuites3")

    val cipherSuites4 = CipherSuites.fromString("a::b")
    assert(cipherSuites4 == CipherSuites.Enabled(Seq("a", "b")), "cipherSuites4")
  }

  test("fromString handles multiple ciphers") {
    val ciphers = "abc:def:ghi:jkl"
    val cipherSuites = CipherSuites.fromString(ciphers)

    val suites: Seq[String] = cipherSuites match {
      case CipherSuites.Enabled(list) => list
      case _ => Seq.empty
    }

    assert(suites == Seq("abc", "def", "ghi", "jkl"))
  }

}
