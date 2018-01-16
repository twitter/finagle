package com.twitter.finagle.http.netty3

import com.twitter.conversions.time._
import com.twitter.finagle.http.{Cookie, CookieMap, CookieMapTest, Message, Request, Response}

// These tests exercise common N3/N4 cookie behavior via CookieMapTest, and then further
// test Netty 3-specific behavior below.
class Netty3CookieMapTest extends CookieMapTest(Netty3CookieCodec, "netty 3") {

  def testCookies(newMessage: () => Message, headerName: String, messageType: String): Unit = {
    test(s"Adding a $messageType cookie header with attributes adds the cookie with the " +
      "attributes") {
      val message = newMessage()
      lazy val cookieMap = new CookieMap(message, Netty3CookieCodec)
      message.headerMap.set(headerName, "name=value; Max-Age=23; Domain=.example.com; Path=/")
      val cookie = cookieMap("name")

      assert(cookie.value == "value")
      assert(cookie.maxAge == 23.seconds)
      assert(cookie.domain == ".example.com")
      assert(cookie.path == "/")
    }
  }

  // Request cookie tests
  testCookies(() => Request(), "Cookie", "Request")

  test(s"Only Domain and Path attributes on a Request cookie are added to the header") {
    val request = Request()
    val cookie = new Cookie(
      name = "name",
      value = "value",
      domain = Some("foo"),
      path = Some("bar"),
      maxAge = Some(5.minutes),
      secure = true,
      httpOnly = true
    )

    lazy val cookieMap = new CookieMap(request, Netty3CookieCodec)
    cookieMap += cookie
    val headers = request.headerMap("Cookie")
    assert(headers.contains("name=value"))
    assert(headers.contains("Domain=foo"))
    assert(headers.contains("Path=bar"))
    assert(!headers.contains("Expires"))
    assert(!headers.contains("Secure"))
    assert(!headers.contains("HTTPOnly"))
  }

  // Response cookie tests
  testCookies(() => Response(), "Set-Cookie", "Response")

  test(s"Setting multiple cookies in a single header on a Response adds all the cookies") {
    val response = Response()
    lazy val cookieMap = new CookieMap(response, Netty3CookieCodec)
    response.headerMap.set("Set-Cookie", "name=value; name2=value2")
    assert(cookieMap("name").value == "value")
    assert(cookieMap("name2").value == "value2")
    assert(cookieMap.isValid == true)

    val cookieHeaders = response.headerMap.getAll("Set-Cookie")
    assert(cookieHeaders.size == 2)
    assert(cookieHeaders.toSeq.contains("name=value"))
    assert(cookieHeaders.toSeq.contains("name2=value2"))
  }
}
