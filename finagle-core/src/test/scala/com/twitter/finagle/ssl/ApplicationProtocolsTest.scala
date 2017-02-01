package com.twitter.finagle.ssl

import org.scalatest.FunSuite

class ApplicationProtocolsTest extends FunSuite {

  test("Supported with alpn or npn values succeeds") {
    val appProtos = ApplicationProtocols.Supported(Seq("h2", "http/1.1"))
    assert(appProtos == ApplicationProtocols.Supported(Seq("h2", "http/1.1")))
  }

  test("Supported with non alpn or npn values fails") {
    intercept[IllegalArgumentException] {
      val appProtos = ApplicationProtocols.Supported(Seq("h2", "test", "http/1.1"))
    }
  }

  test("fromString results in unspecified when application protocols are empty") {
    val appProtos = ApplicationProtocols.fromString("")
    assert(appProtos == ApplicationProtocols.Unspecified)
  }

  test("fromString drops empty application protocols") {
    val appProtos1 = ApplicationProtocols.fromString(",")
    assert(appProtos1 == ApplicationProtocols.Unspecified, "appProtos1")

    val appProtos2 = ApplicationProtocols.fromString("h2,")
    assert(appProtos2 == ApplicationProtocols.Supported(Seq("h2")), "appProtos2")

    val appProtos3 = ApplicationProtocols.fromString(",spdy/3.1")
    assert(appProtos3 == ApplicationProtocols.Supported(Seq("spdy/3.1")), "appProtos3")

    val appProtos4 = ApplicationProtocols.fromString("h2,spdy/3.1")
    assert(appProtos4 == ApplicationProtocols.Supported(Seq("h2", "spdy/3.1")), "appProtos4")
  }

  test("fromString handles multiple application protocols") {
    val appProtos = ApplicationProtocols.fromString("h2,spdy/3.1,h2c,http/1.1")

    val items: Seq[String] = appProtos match {
      case ApplicationProtocols.Supported(list) => list
      case _ => Seq.empty
    }

    assert(items == Seq("h2", "spdy/3.1", "h2c", "http/1.1"))
  }

  test("fromString handles multiple application protocols with spaces") {
    val appProtos = ApplicationProtocols.fromString("h2, spdy/3.1 , http/1.1")

    val items: Seq[String] = appProtos match {
      case ApplicationProtocols.Supported(list) => list
      case _ => Seq.empty
    }

    assert(items == Seq("h2", "spdy/3.1", "http/1.1"))
  }

  test("fromString with non alpn or npn values fails") {
    intercept[IllegalArgumentException] {
      val appProtos = ApplicationProtocols.fromString("h2, test, spdy/3.1, what")
    }
  }

}
