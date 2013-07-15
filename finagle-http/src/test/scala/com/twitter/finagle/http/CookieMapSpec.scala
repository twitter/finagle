package com.twitter.finagle.http

import com.twitter.conversions.time._
import org.specs.SpecificationWithJUnit

class CookieMapSpec extends SpecificationWithJUnit {
  "CookieMap" should {
    "no cookies" in {
      val request = Request()
      request.cookies must beEmpty
    }

    "request cookie basics" in {
      val request = Request()
      request.headers("Cookie") = "name=value; name2=value2"
      request.cookies("name").value must_== "value"
      request.cookies("name2").value must_== "value2"
      request.cookies.isValid must beTrue
    }

    "response cookie basics" in {
      val response = Response()
      response.headers("Set-Cookie") = "name=value; name2=value2"
      response.cookies("name").value must_== "value"
      response.cookies("name2").value must_== "value2"
    }

    "cookie with attributes" in {
      val request = Request()
      request.headers("Cookie") = "name=value; Max-Age=23; Domain=.example.com; Path=/"
      val cookie = request.cookies("name")
      cookie.value  must_== "value"
      cookie.maxAge must_== 23.seconds
      cookie.domain must_== ".example.com"
      cookie.path   must_== "/"
    }

    "add cookie" in {
      val request = Request()
      val cookie = new Cookie("name", "value")
      request.cookies += cookie
      request.cookies("name").value must_== "value"
      request.headers("Cookie") must_== "name=value"
    }

    "add same cookie only once" in {
      val request = Request()
      val cookie = new Cookie("name", "value")
      request.cookies += cookie
      request.cookies += cookie
      request.cookies("name").value must_== "value"
      request.headers("Cookie") must_== "name=value"
      request.cookies must haveSize(1)
    }

    "add same cookie more than once" in {
      val request = Request()
      val cookie = new Cookie("name", "value")
      val cookie2 = new Cookie("name", "value2")
      request.cookies.add(cookie)
      request.cookies.add(cookie2)

      request.cookies must haveSize(2)
      request.cookies("name").value must_== "value"

      val cookieHeaders = request.headers.getAll("Cookie")
      cookieHeaders must haveSize(2)
      cookieHeaders must contain ("name=value")
      cookieHeaders must contain ("name=value2")
    }

    "remove cookie" in {
      val request = Request()
      request.headers.add("Cookie", "name=value")
      request.headers.add("Cookie", "name=value2") // same name - gets removed too

      request.cookies -= "name"
      request.cookies must haveSize(0)
    }

    "invalid cookies are ignored" in {
      val request = Request()
      request.headers.add("Cookie", "namé=value")
      request.cookies must haveSize(0)
      request.cookies.isValid must beFalse
    }
  }
}
