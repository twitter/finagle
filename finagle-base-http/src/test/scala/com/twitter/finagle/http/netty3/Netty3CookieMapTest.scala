package com.twitter.finagle.http.netty3

import com.twitter.conversions.time._
import com.twitter.finagle.http.cookie.SameSite
import com.twitter.finagle.http.cookie.exp.supportSameSiteCodec
import com.twitter.finagle.http.{Cookie, CookieMap, CookieMapTest, Message, Request, Response}

// These tests exercise common N3/N4 cookie behavior via CookieMapTest, and then further
// test Netty 3-specific behavior below.
class Netty3CookieMapTest extends CookieMapTest(Netty3CookieCodec, "netty 3") {

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

  def testCookies(newMessage: () => Message, headerName: String, messageType: String): Unit = {
    toggledTest(s"Adding a $messageType cookie header with attributes adds the cookie with the " +
      "attributes") {
      val message = newMessage()
      lazy val cookieMap = new CookieMap(message, Netty3CookieCodec)
      message.headerMap.set(headerName, "name=value; Max-Age=23; Domain=.example.com; Path=/")
      val cookie = cookieMap("name")

      assert(cookie.value == "value")
      assert(cookie.maxAge == 23.seconds)
      assert(cookie.domain == ".example.com")
      assert(cookie.path == "/")
      assert(cookie.sameSite == SameSite.Unset)
    }
  }

  // Request cookie tests
  testCookies(() => Request(), "Cookie", "Request")

  toggledTest(s"Only Domain and Path attributes on a Request cookie are added to the header") {
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
    assert(!headers.contains("SameSite"))
  }

  // Response cookie tests
  testCookies(() => Response(), "Set-Cookie", "Response")

  toggledTest(s"Setting multiple cookies in a single header on a Response adds all the cookies") {
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
    assert(!cookieHeaders.toSeq.contains("SameSite="))
  }

  test("decodeClient properly handles multiple cookies") {
    for {
      sameSite <- Seq(SameSite.Lax, SameSite.Strict, SameSite.Unset)
    } {
      supportSameSiteCodec.let(true) {
        val response = Response()
        response.cookies.add(new Cookie("foo", "bar", sameSite = sameSite))
        response.cookies.add(new Cookie("baz", "qux"))
        response.cookies.add(new Cookie("quux", "quuz"))

        val results = List(
          List("baz", "qux", SameSite.Unset),
          List("foo", "bar", sameSite),
          List("quux", "quuz", SameSite.Unset)
        )

        val cookieHeaders = response.headerMap.getAll("Set-Cookie")
        assert(cookieHeaders.size == 3)
        var idx: Int = 0

        // Mimic behavior of the CookieMap constructor
        response.headerMap.getAll("Set-Cookie").foreach { header: String =>
          val cookieOpt: Option[Iterable[Cookie]] = Netty3CookieCodec.decodeClient(header)
          assert(cookieOpt.isDefined)
          cookieOpt.get.foreach { cookie =>
            assert(cookie.name == results(idx)(0))
            assert(cookie.value == results(idx)(1))
            assert(cookie.sameSite == results(idx)(2).asInstanceOf[SameSite])
            idx += 1
          }
        }
      }
    }
  }

  def sameSiteToString(s: SameSite): String = s match {
    case SameSite.Lax => " SameSite=Lax;"
    case SameSite.Strict => " SameSite=Strict;"
    case _ => ""
  }

  test("decodeClient properly handles multiple cookies in one Set-Cookie header") {
    for {
      sameSite <- Seq(SameSite.Lax, SameSite.Strict, SameSite.Unset)
    } {
      supportSameSiteCodec.let(true) {
        val response = Response()

        response.headerMap.set("Set-Cookie", s"foo=bar; baz=qux;${sameSiteToString(sameSite)} quux=quuz")

        val results = List(
          List("quux", "quuz", SameSite.Unset),
          List("foo", "bar", SameSite.Unset),
          List("baz", "qux", sameSite)
        )

        val cookieHeaders = response.headerMap.getAll("Set-Cookie")
        assert(cookieHeaders.size == 1)
        var idx: Int = 0

        // Mimic behavior of the CookieMap constructor
        response.headerMap.getAll("Set-Cookie").foreach { header: String =>
          val cookieOpt: Option[Iterable[Cookie]] = Netty3CookieCodec.decodeClient(header)
          assert(cookieOpt.isDefined)
          cookieOpt.get.foreach { cookie =>
            assert(cookie.name == results(idx)(0))
            assert(cookie.value == results(idx)(1))
            assert(cookie.sameSite == results(idx)(2).asInstanceOf[SameSite])
            idx += 1
          }
        }
      }
    }
  }
}
