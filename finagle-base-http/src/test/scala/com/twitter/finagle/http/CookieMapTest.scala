package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import org.scalatest.funsuite.AnyFunSuite

abstract class CookieMapTest(codec: CookieCodec, codecName: String) extends AnyFunSuite {

  private[this] def testCookies(
    newMessage: () => Message,
    headerName: String,
    messageType: String
  ): Unit = {
    test(s"$codec: CookieMap for $messageType initially has no cookies") {
      val message = newMessage()
      val cookieMap = new CookieMap(message, codec)
      assert(cookieMap.isEmpty)
    }

    test(s"$codec: Accessing cookies of a $messageType does not change its header") {
      val message = newMessage()
      message.cookies
      assert(message.headerMap.isEmpty)
    }

    test(s"$codec: Invalid cookies on a $messageType are ignored") {
      val message = newMessage()
      lazy val cookieMap = new CookieMap(message, codec)
      message.headerMap.add(headerName, "namé=value")

      assert(cookieMap.size == 0)
      assert(cookieMap.isValid == false)
    }

    test(s"$codec: Adding a cookie to the CookieMap on a $messageType adds it to the header") {
      val message = newMessage()
      lazy val cookieMap = new CookieMap(message, codec)
      val cookie = new Cookie("name", "value")
      cookieMap += cookie
      assert(cookieMap("name").value == "value")
      assert(message.headerMap(headerName) == "name=value")
    }

    test(
      s"$codec: Add multiple cookies to the CookieMap on a $messageType adds them to the header") {
      val message = newMessage()
      val allPossibleHeaders = Seq("foo=foo", "bar=bar", "baz=baz").permutations
        .map(_.mkString("; "))
        .toSeq

      val cookieMap = new CookieMap(message, codec)
      val foo = new Cookie("foo", "foo")
      val bar = new Cookie("bar", "bar")
      val baz = new Cookie("baz", "baz")

      cookieMap ++= Seq("foo" -> foo, "bar" -> bar, "baz" -> baz)
      assert(cookieMap.size == 3)
      assert(allPossibleHeaders.contains(message.headerMap.getAll(headerName).mkString("; ")))

      cookieMap --= Seq("foo", "bar", "baz")
      assert(cookieMap.isEmpty)
      assert(message.headerMap.get(headerName).isEmpty || message.headerMap(headerName) == "")

      cookieMap.addAll(Seq(foo, bar, baz))
      assert(cookieMap.size == 3)
      assert(allPossibleHeaders.contains(message.headerMap.getAll(headerName).mkString("; ")))

      cookieMap.removeAll(Seq("foo", "bar", "baz"))
      assert(cookieMap.isEmpty)
      assert(message.headerMap.get(headerName).isEmpty || message.headerMap(headerName) == "")
    }

    test(
      s"$codec: Adding the same cookie object to a CookieMap on a $messageType more than once " +
        s"results in a $messageType with one cookie"
    ) {
      val message = newMessage()
      lazy val cookieMap = new CookieMap(message, codec)
      val cookie = new Cookie("name", "value")
      cookieMap += cookie
      cookieMap += cookie

      assert(cookieMap.size == 1)
      assert(cookieMap("name").value == "value")
      assert(message.headerMap(headerName) == "name=value")
    }

    test(
      s"$codec: Adding different cookie objects with the same name to a CookieMap on a " +
        s"$messageType more than once, using +=, results in a CookieMap with one cookie"
    ) {
      val message = newMessage()
      val cookie = new Cookie("name", "value")
      val cookie2 = new Cookie("name", "value2")
      lazy val cookieMap = new CookieMap(message, codec)
      cookieMap += cookie
      cookieMap += cookie2

      assert(cookieMap.size == 1)
      // We expect to see the recently added cookie
      assert(cookieMap("name").value == "value2")
      assert(message.headerMap(headerName) == "name=value2")
    }

    test(
      s"$codec Adding two cookies with the same name but different domain to a $messageType " +
        "adds both cookies"
    ) {
      val message = newMessage()
      val cookie = new Cookie("name", "value").domain(Some("foo"))
      val cookie2 = new Cookie("name", "value2").domain(Some("bar"))
      lazy val cookieMap = new CookieMap(message, codec)

      cookieMap.add(cookie)
      cookieMap.add(cookie2)

      assert(cookie !== cookie2)
      assert(cookieMap.size == 2)
      val cookies = cookieMap.getAll("name")
      assert(cookies.contains(cookie))
      assert(cookies.contains(cookie2))
    }

    test(s"$codec: Removing cookies by name on a $messageType removes all cookies with that name") {
      val message = newMessage()
      message.headerMap.add("Cookie", "name=value")
      message.headerMap.add("Cookie", "name=value2") // same name - gets removed too
      lazy val cookieMap = new CookieMap(message, codec)
      cookieMap -= "name"

      assert(cookieMap.size == 0)
    }

    test(s"$codec: Removing all cookies of a $messageType also removes its Cookie header") {
      val message = newMessage()
      message.headerMap.add("Cookie", "name=value")
      lazy val cookieMap = new CookieMap(message, codec)
      cookieMap -= "name"

      assert(!message.headerMap.contains(headerName))
    }
  }

  // Request tests
  testCookies(() => Request(), "Cookie", "Request")

  test(
    s"$codec: Adding different cookie objects with the same name to a CookieMap on a " +
      s"Request more than once, using add, results in a CookieMap with more than one cookie"
  ) {
    val message = Request()
    val cookie = new Cookie("name", "value")
    val cookie2 = new Cookie("name", "value2")
    lazy val cookieMap = new CookieMap(message, codec)
    cookieMap.add(cookie)
    cookieMap.add(cookie2)

    assert(cookieMap.size == 2)
    // We expect to see both cookies
    assert(cookieMap.getAll("name").map(_.value) == Seq("value", "value2"))
    val cookies = message.headerMap("Cookie")
    assert(cookies == "name=value; name=value2")
  }

  test("Setting multiple cookies on a Request in a single header adds all the cookies") {
    val request = Request()
    request.headerMap.set("Cookie", "name=value; name2=value2")
    lazy val cookieMap = new CookieMap(request, codec)
    assert(cookieMap("name").value == "value")
    assert(cookieMap("name2").value == "value2")
    assert(cookieMap.isValid == true)

    val cookies = request.headerMap("Cookie")

    // This is inelegant but we also want to make sure there's a ; between the cookies,
    // so this is easiest since ordering is not deterministic.
    assert(cookies == "name=value; name2=value2" || cookies == "name2=value2; name=value")
  }

  test(
    "Setting multiple cookies on a Request in a single header with the same name but " +
      "different values adds all the cookies"
  ) {
    val request = Request()
    request.headerMap.set("Cookie", "name=value2; name=value;")

    val cookie = new Cookie("name", "value")
    val cookie2 = new Cookie("name", "value2")
    lazy val cookieMap = new CookieMap(request, codec)

    assert(cookieMap.values.toSet == Set(cookie, cookie2))
  }

  test("Adding a cookie to a Request with an existing cookie adds it to the header and cookies using +=") {
    val request = Request()
    request.headerMap.set("Cookie", "name=value")
    lazy val cookieMap = new CookieMap(request, codec)

    val cookie2 = new Cookie("name2", "value2")
    val cookie2bis = new Cookie("name2", "value2bis")
    cookieMap += cookie2
    cookieMap += cookie2bis
    val cookies = request.headerMap("Cookie")
    assert(cookies == "name=value; name2=value2bis" || cookies == "name2=value2bis; name=value")
  }

  test("Adding a cookie to a Request with an existing cookie adds it to the header and cookies using add") {
    val request = Request()
    request.headerMap.set("Cookie", "name=value")
    lazy val cookieMap = new CookieMap(request, codec)

    val cookie2 = new Cookie("name2", "value2")
    val cookie2bis = new Cookie("name2", "value2bis")
    cookieMap.add(cookie2)
    cookieMap.add(cookie2bis)
    val cookies = request.headerMap("Cookie")
    assert(cookies == "name=value; name2=value2; name2=value2bis" || cookies == "name2=value2; name2=value2bis; name=value")
  }

  // Response tests
  testCookies(() => Response(), "Set-Cookie", "Response")

  test(
    s"$codec: Adding different cookie objects with the same name to a CookieMap on a " +
      s"Response more than once, using add, results in a CookieMap with one cookie"
  ) {
    val message = Response()
    val cookie = new Cookie("name", "value")
    val cookie2 = new Cookie("name", "value2")
    lazy val cookieMap = new CookieMap(message, codec)
    cookieMap.add(cookie)
    cookieMap.add(cookie2)

    assert(cookieMap.size == 1)
    // We expect to see the recently added cookie
    assert(cookieMap("name").value == "value2")
    assert(message.headerMap("Set-Cookie") == "name=value2")
  }

  test(s"$codec: Adding multiple Set-Cookie headers to a Response adds those cookies") {
    val response = Response()
    response.headerMap.add("Set-Cookie", "name=value")
    response.headerMap.add("Set-Cookie", "name2=value2")
    lazy val cookieMap = new CookieMap(response, codec)
    assert(cookieMap("name").value == "value")
    assert(cookieMap("name2").value == "value2")
  }

  test(s"$codec: Adding multiple cookies to a Response adds them to the headerMap") {
    val response = Response()

    val cookie = new Cookie("name", "value")
    val cookie2 = new Cookie("name2", "value2")
    lazy val cookieMap = new CookieMap(response, codec)

    cookieMap += cookie
    cookieMap += cookie2
    val cookieHeaders = response.headerMap.getAll("Set-Cookie")
    assert(cookieHeaders.size == 2)
    assert(cookieHeaders.toSeq.contains("name=value"))
    assert(cookieHeaders.toSeq.contains("name2=value2"))
  }

  test(s"$codec: All attributes on a Response cookie are added to the header") {
    val response = Response()
    val cookie = new Cookie(
      name = "name",
      value = "value",
      domain = Some("foo"),
      path = Some("bar"),
      maxAge = Some(5.minutes),
      secure = true,
      httpOnly = true
    )

    lazy val cookieMap = new CookieMap(response, codec)
    cookieMap += cookie
    val headers = response.headerMap("Set-Cookie")
    assert(headers.contains("name=value"))
    assert(headers.contains("Domain=foo"))
    assert(headers.contains("Path=bar"))
    assert(headers.contains("Expires"))
    assert(headers.contains("Secure"))
    assert(headers.contains("HTTPOnly"))
  }

  test(
    s"$codec: Adding multiple Set-Cookie headers to a Response with the same name but " +
      "different value adds only one Cookie"
  ) {
    val response = Response()
    response.headerMap.add("Set-Cookie", "name=value")
    response.headerMap.add("Set-Cookie", "name=value2")
    lazy val cookieMap = new CookieMap(response, codec)
    val cookies = cookieMap.getAll("name")
    assert(cookies.size == 1)
    // we should only see the most recent one
    assert(cookies.contains(new Cookie("name", "value2")))
  }

  test(
    s"$codec: Adding a cookie to a Response with an existing cookie adds it to the header " +
      "and cookies"
  ) {
    val response = Response()
    response.headerMap.set("Set-Cookie", "name=value")
    lazy val cookieMap = new CookieMap(response, codec)

    val cookie = new Cookie("name2", "value2")
    cookieMap += cookie
    assert(cookieMap.get("name").get.value == "value")
    assert(cookieMap.get("name2").get.value == "value2")
    val cookies = response.headerMap.getAll("Set-Cookie")
    assert(cookies.contains("name=value"))
    assert(cookies.contains("name2=value2"))
  }
}
