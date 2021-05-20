package com.twitter.finagle.http.netty4

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.Cookie
import com.twitter.finagle.http.cookie.{SameSite, supportSameSiteCodec}
import com.twitter.finagle.http.netty4.Netty4CookieCodec._
import io.netty.handler.codec.http.cookie.{
  Cookie => NettyCookie,
  DefaultCookie => NettyDefaultCookie
}
import org.scalatest.funsuite.AnyFunSuite

class Netty4CookieCodecTest extends AnyFunSuite {

  def toggledTest(what: String)(f: => Unit) = {
    for {
      sameSiteCodec <- Seq(false, true)
    } {
      test(s"$what with flag supportSameSiteCodec = $sameSiteCodec") {
        supportSameSiteCodec.let(sameSiteCodec) {
          f
        }
      }
    }
  }

  // N4 STRICT mode does not permit control characters (0 - 32, 127), whitespace, double-quote, comma,
  // semicolon, backslash, or non-ascii characters (here we know that we test up to 150.toChar)
  val prohibitedInN4 =
    (0 to 32).map(_.toChar) ++
      Seq(127.toChar) ++
      Seq(' ', '"', ',', ';', '\\') ++
      (128 to 150).map(_.toChar)

  toggledTest("finagle cookie with name, value -> netty") {
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

  toggledTest("finagle cookie with sameSite -> netty") {
    val in = new Cookie(
      name = "name",
      value = "value",
      sameSite = SameSite.Lax
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

  toggledTest("finagle cookie with domain, path -> netty") {
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

  toggledTest("finagle cookie with isHttpOnly, isSecure -> netty") {
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

  toggledTest("finagle cookie with maxAge -> netty") {
    val in = new Cookie(
      name = "name",
      value = "value",
      maxAge = Some(5.minutes)
    )
    val out = cookieToNetty(in)
    assert(out.maxAge == 5.minutes.inSeconds)
  }

  toggledTest("netty cookie with name, value -> finagle") {
    val in = new NettyDefaultCookie("name", "value")
    val out = cookieToFinagle(in)
    assert(out.name == "name")
    assert(out.value == "value")
    assert(out.httpOnly == false)
    assert(out.secure == false)
    assert(out.path == null)
    assert(out.domain == null)
    assert(out.maxAge == Cookie.DefaultMaxAge)
    assert(out.sameSite == SameSite.Unset)
  }

  toggledTest("netty cookie with domain, path -> finagle") {
    val in = new NettyDefaultCookie("name", "value")
    in.setDomain("domain")
    in.setPath("path")
    val out = cookieToFinagle(in)
    assert(out.domain == "domain")
    assert(out.path == "path")
    assert(out.sameSite == SameSite.Unset)
  }

  toggledTest("netty cookie with isHttpOnly, isSecure -> finagle") {
    val in = new NettyDefaultCookie("name", "value")
    in.setSecure(true)
    in.setHttpOnly(true)
    val out = cookieToFinagle(in)
    assert(out.httpOnly == true)
    assert(out.secure == true)
    assert(out.sameSite == SameSite.Unset)
  }

  toggledTest("netty cookie with maxAge -> finagle") {
    val in = new NettyDefaultCookie("name", "value")
    in.setMaxAge(5.minutes.inSeconds)
    val out = cookieToFinagle(in)
    assert(out.maxAge == 5.minutes)
    assert(out.sameSite == SameSite.Unset)
  }
}
