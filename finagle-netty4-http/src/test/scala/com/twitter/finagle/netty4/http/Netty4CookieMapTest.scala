package com.twitter.finagle.netty4.http
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.netty4.Netty4CookieCodec
import com.twitter.finagle.http.{Cookie, CookieMap, CookieMapTest, Request, Response}

class Netty4CookieMapTest extends CookieMapTest(Netty4CookieCodec, "netty 4") {

  test("Attributes on Request cookies headers are ignored") {
    val request = Request()
    lazy val cookieMap = new CookieMap(request, Netty4CookieCodec)
    request.headerMap.set("Cookie", "name=value; Max-Age=23; Domain=.example.com; Path=/")
    val cookie = cookieMap("name")

    assert(cookie.value == "value")
    assert(cookie.maxAge == Cookie.DefaultMaxAge)
    assert(cookie.domain == null)
    assert(cookie.path == null)
  }

  test("Attributes on Request cookies are not added to the header") {
    val request = Request()
    lazy val cookieMap = new CookieMap(request, Netty4CookieCodec)
    val cookie = new Cookie(
      name = "name",
      value = "value",
      domain = Some("foo"),
      path = Some("bar"),
      maxAge = Some(5.minutes),
      secure = true,
      httpOnly = true
    )

    cookieMap += cookie
    assert(request.headerMap("Cookie") == "name=value")
  }

  test("Setting multiple cookies on a Response in a single header only adds the first cookie") {
    val response = Response()
    lazy val cookieMap = new CookieMap(response, Netty4CookieCodec)
    response.headerMap.set("Set-Cookie", "name=value; name2=value2")
    assert(cookieMap.size == 1)
    assert(cookieMap("name").value == "value")
  }
}
