package com.twitter.finagle.http

import org.scalatest.funsuite.AnyFunSuite

class VersionTest extends AnyFunSuite {

  test("version string") {
    Version.Http10.versionString == "HTTP/1.0"
    Version.Http10.toString == "HTTP/1.0"

    Version.Http11.versionString == "HTTP/1.1"
    Version.Http11.toString == "HTTP/1.1"
  }

  test("Major and minor values") {
    assert(Version.Http10.major == 1)
    assert(Version.Http10.minor == 0)

    assert(Version.Http11.major == 1)
    assert(Version.Http11.minor == 1)
  }

  test("pattern matching based on value") {
    val v: Version = Version.Http10
    v match {
      case Version.Http10 => assert(true)
      case _ => fail()
    }
  }

  test("pattern matching version numbers") {
    Version.Http10 match {
      case Version(1, 0) => assert(true)
      case _ => fail()
    }
  }
}
