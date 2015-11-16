package com.twitter.finagle.http
import com.twitter.conversions.time._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CookieMapTest extends FunSuite {

  test("no cookies") {
    val request = Request()
    request.cookies.isEmpty == true
  }

  test("request cookie basics") {
    val request = Request()
    request.headers.set("Cookie", "name=value; name2=value2")
    assert(request.cookies("name").value == "value")
    assert(request.cookies("name2").value == "value2")
    assert(request.cookies.isValid == true)

    val cookie = new Cookie("name3", "value3")
    request.cookies += cookie
    assert(request.headers.get("Cookie") == "name=value; name2=value2; name3=value3")
  }

  test("response cookie basics") {
    val response = Response()
    response.headers.add("Set-Cookie", "name=value")
    response.headers.add("Set-Cookie", "name2=value2")
    assert(response.cookies("name").value == "value")
    assert(response.cookies("name2").value == "value2")

    val cookie = new Cookie("name3", "value3")
    response.cookies += cookie
    val cookieHeaders = response.headerMap.getAll("Set-Cookie")
    assert(cookieHeaders.size == 3)
    assert(cookieHeaders.toSeq.contains("name=value"))
    assert(cookieHeaders.toSeq.contains("name2=value2"))
    assert(cookieHeaders.toSeq.contains("name3=value3"))
  }

  test("cookie with attributes") {
    val request = Request()
    request.headers.set("Cookie", "name=value; Max-Age=23; Domain=.example.com; Path=/")
    val cookie = request.cookies("name")

    assert(cookie.value == "value")
    assert(cookie.maxAge == 23.seconds)
    assert(cookie.domain == ".example.com")
    assert(cookie.path == "/")
  }

  test("add cookie") {
    val request = Request()
    val cookie = new Cookie("name", "value")
    request.cookies += cookie
    assert(request.cookies("name").value == "value")
    assert(request.headers.get("Cookie") == "name=value")
  }

  test("add same cookie only once") {
    val request = Request()
    val cookie = new Cookie("name", "value")
    request.cookies += cookie
    request.cookies += cookie

    assert(request.cookies.size == 1)
    assert(request.cookies("name").value == "value")
    assert(request.headers.get("Cookie") == "name=value")
  }

  test("add same cookie more than once") {
    val request = Request()
    val cookie = new Cookie("name", "value")
    val cookie2 = new Cookie("name", "value2")
    request.cookies.add(cookie)
    request.cookies.add(cookie2)

    assert(request.cookies.size == 1)
    // We expect to see the recently added cookie
    assert(request.cookies("name").value == "value2")
    assert(request.headers.get("Cookie") == "name=value2")
  }

  test("add cookies with the same name but different domain") {
    val request = Request()
    val cookie = new Cookie("name", "value")
    cookie.domain = "foo"
    val cookie2 = new Cookie("name", "value2")
    cookie2.domain = "bar"

    request.cookies.add(cookie)
    request.cookies.add(cookie2)

    assert(cookie !== cookie2)
    assert(request.cookies.size == 2)
    assert(request.cookies("name").value == "value")
    assert(request.headers.get("Cookie") == "name=value2; $Domain=bar; name=value; $Domain=foo")
  }

  test("parse header with two cookies with the same name") {
    val request = Request()
    request.headers.set("Cookie", "name=value2; name=value;")

    val cookie = new Cookie("name", "value")
    val cookie2 = new Cookie("name", "value2")

    assert(request.cookies.values.toSet == Set(cookie, cookie2))
  }

  test("remove cookie") {
    val request = Request()
    request.headers.add("Cookie", "name=value")
    request.headers.add("Cookie", "name=value2") // same name - gets removed too
    request.cookies -= "name"

    assert(request.cookies.size == 0)
  }

  test("invalid cookies are ignored") {
    val request = Request()
    request.headers.add("Cookie", "namé=value")

    assert(request.cookies.size == 0)
    assert(request.cookies.isValid == false)
  }
}
