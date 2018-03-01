package com.twitter.finagle.http.netty3

import com.twitter.conversions.time._
import com.twitter.finagle.http.Cookie
import com.twitter.finagle.http.netty3.Bijections._
import org.jboss.netty.handler.codec.http.{DefaultCookie, Cookie => NettyCookie}
import org.scalatest.FunSuite

class BijectionsTest extends FunSuite {

  test("finagle cookie -> netty cookie") {
    val in: Cookie = new Cookie(
      name = "name",
      value = "value",
      domain = Some("domain"),
      path = Some("path"),
      maxAge = Some(99.seconds),
      secure = true,
      httpOnly = false
    )

    val out: NettyCookie = Bijections.from(in)
    assert(out.getName == "name")
    assert(out.getValue == "value")
    assert(out.getDomain == "domain")
    assert(out.getPath == "path")
    assert(out.getMaxAge == 99)
    assert(out.isSecure)
    assert(!out.isHttpOnly)

    val in2 = Bijections.from(out)
    assert(in == in2)
  }

  test("netty cookie -> finagle cookie") {
    val in: NettyCookie = new DefaultCookie("name", "value")
    in.setDomain("domain")
    in.setPath("path")
    in.setMaxAge(99)
    in.setSecure(true)
    in.setHttpOnly(false)

    val out = Bijections.from(in)

    assert(out.name == "name")
    assert(out.value == "value")
    assert(out.domain == "domain")
    assert(out.path == "path")
    assert(out.maxAge == 99.seconds)
    assert(out.secure)
    assert(!out.httpOnly)

    val in2 = Bijections.from(out)
    assert(in == in2)
  }
}
