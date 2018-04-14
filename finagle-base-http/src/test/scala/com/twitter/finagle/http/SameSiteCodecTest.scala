package com.twitter.finagle.http

import com.twitter.finagle.http.cookie.{SameSite, SameSiteCodec}
import com.twitter.finagle.http.netty3.Bijections
import com.twitter.finagle.http.netty3.Bijections.cookieToNettyInjection
import org.jboss.netty.handler.codec.http.{CookieEncoder => NettyCookieEncoder}
import org.jboss.netty.handler.codec.http.{Cookie => Netty3Cookie}
import org.scalatest.FunSuite


class SameSiteCodecTest extends FunSuite {

  test("encodeSameSite without sameSite") {
    val encoder = new NettyCookieEncoder(false)
    val cookie = new Cookie("foo", "bar")
    encoder.addCookie(Bijections.from[Cookie, Netty3Cookie](cookie))
    val header = encoder.encode()
    val newHeader = SameSiteCodec.encodeSameSite(cookie, header)
    assert(newHeader == "foo=bar")
  }

  test("encodeSameSite with sameSite = None") {
    val encoder = new NettyCookieEncoder(false)
    val cookie = new Cookie("foo", "bar").sameSite(SameSite.Unset)
    encoder.addCookie(Bijections.from[Cookie, Netty3Cookie](cookie))
    val header = encoder.encode()
    val newHeader = SameSiteCodec.encodeSameSite(cookie, header)
    assert(newHeader == "foo=bar")
  }

  test("encodeSameSite with sameSite = Lax") {
    val encoder = new NettyCookieEncoder(false)
    val cookie = new Cookie("foo", "bar").sameSite(SameSite.Lax)
    encoder.addCookie(Bijections.from[Cookie, Netty3Cookie](cookie))
    val header = encoder.encode()
    val newHeader = SameSiteCodec.encodeSameSite(cookie, header)
    assert(newHeader == "foo=bar; SameSite=Lax")
  }

  test("encodeSameSite with sameSite = Strict") {
    val encoder = new NettyCookieEncoder(false)
    val cookie = new Cookie("foo", "bar").sameSite(SameSite.Strict)
    encoder.addCookie(Bijections.from[Cookie, Netty3Cookie](cookie))
    val header = encoder.encode()
    val newHeader = SameSiteCodec.encodeSameSite(cookie, header)
    assert(newHeader == "foo=bar; SameSite=Strict")
  }

  test("decodeSameSite without sameSite") {
    // Note that `decodeSameSite` is called by `decodeClient`, which
    // decodes only `Response`s.
    val message = Response()
    val cookie = new Cookie("foo", "bar")
    message.cookies += cookie
    val header = message.headerMap.get("Set-Cookie").get
    val newCookie = SameSiteCodec.decodeSameSite(header, cookie)
    assert(newCookie.name == "foo")
    assert(newCookie.value == "bar")
    assert(newCookie.sameSite == SameSite.Unset)
  }

  test("decodeSameSite with sameSite = None") {
    val message = Response()
    val cookie = new Cookie("foo", "bar").sameSite(SameSite.Unset)
    message.cookies += cookie
    val header = message.headerMap.get("Set-Cookie").get
    val newCookie = SameSiteCodec.decodeSameSite(header, cookie)
    assert(newCookie.name == "foo")
    assert(newCookie.value == "bar")
    assert(newCookie.sameSite == SameSite.Unset)
  }

  test("decodeSameSite with sameSite = Lax") {
    val message = Response()
    val cookie = new Cookie("foo", "bar").sameSite(SameSite.Lax)
    message.cookies += cookie
    val header = message.headerMap.get("Set-Cookie").get
    val newCookie = SameSiteCodec.decodeSameSite(header, cookie)
    assert(newCookie.name == "foo")
    assert(newCookie.value == "bar")
    assert(newCookie.sameSite == SameSite.Lax)
  }

  test("decodeSameSite with sameSite = Strict") {
    val message = Response()
    val cookie = new Cookie("foo", "bar").sameSite(SameSite.Strict)
    message.cookies += cookie
    val header = message.headerMap.get("Set-Cookie").get
    val newCookie = SameSiteCodec.decodeSameSite(header, cookie)
    assert(newCookie.name == "foo")
    assert(newCookie.value == "bar")
    assert(newCookie.sameSite == SameSite.Strict)
  }

}
