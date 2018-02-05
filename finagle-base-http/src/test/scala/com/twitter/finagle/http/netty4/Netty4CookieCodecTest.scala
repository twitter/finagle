package com.twitter.finagle.http.netty4

import com.twitter.conversions.time._
import com.twitter.finagle.http.Cookie
import com.twitter.finagle.http.netty4.Netty4CookieCodec._
import io.netty.handler.codec.http.cookie.{
  Cookie => NettyCookie,
  DefaultCookie => NettyDefaultCookie
}
import org.scalatest.FunSuite

class Netty4CookieCodecTest extends FunSuite {
  test("finagle cookie with name, value -> netty") {
    val in = new Cookie(
      name = "name",
      value = "value"
    )
    val out = cookieToNetty(in)
    assert(out.name == "name")
    assert(out.value == "value")
    assert(out.isHttpOnly == false)
    assert(out.isSecure == false)
    assert(out.path == null)
    assert(out.domain == null)
    assert(out.maxAge == NettyCookie.UNDEFINED_MAX_AGE)
  }

  test("finagle cookie with domain, path -> netty") {
    val in = new Cookie(
      name = "name",
      value = "value",
      domain = Some("domain"),
      path = Some("path")
    )
    val out = cookieToNetty(in)
    assert(out.domain == "domain")
    assert(out.path == "path")
  }

  test("finagle cookie with isHttpOnly, isSecure -> netty") {
    val in = new Cookie(
      name = "name",
      value = "value",
      secure = true,
      httpOnly = true
    )
    val out = cookieToNetty(in)
    assert(out.isHttpOnly == true)
    assert(out.isSecure == true)
  }

  test("finagle cookie with maxAge -> netty") {
    val in = new Cookie(
      name = "name",
      value = "value",
      maxAge = Some(5.minutes)
    )
    val out = cookieToNetty(in)
    assert(out.maxAge == 5.minutes.inSeconds)
  }

  test("netty cookie with name, value -> finagle") {
    val in = new NettyDefaultCookie("name", "value")
    val out = cookieToFinagle(in)
    assert(out.name == "name")
    assert(out.value == "value")
    assert(out.httpOnly == false)
    assert(out.secure == false)
    assert(out.path == null)
    assert(out.domain == null)
    assert(out.maxAge == Cookie.DefaultMaxAge)
  }

  test("netty cookie with domain, path -> finagle") {
    val in = new NettyDefaultCookie("name", "value")
    in.setDomain("domain")
    in.setPath("path")
    val out = cookieToFinagle(in)
    assert(out.domain == "domain")
    assert(out.path == "path")
  }

  test("netty cookie with isHttpOnly, isSecure -> finagle") {
    val in = new NettyDefaultCookie("name", "value")
    in.setSecure(true)
    in.setHttpOnly(true)
    val out = cookieToFinagle(in)
    assert(out.httpOnly == true)
    assert(out.secure == true)
  }

  test("netty cookie with maxAge -> finagle") {
    val in = new NettyDefaultCookie("name", "value")
    in.setMaxAge(5.minutes.inSeconds)
    val out = cookieToFinagle(in)
    assert(out.maxAge == 5.minutes)
  }
}
