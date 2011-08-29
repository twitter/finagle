package com.twitter.finagle.http

import org.specs.Specification
import org.jboss.netty.handler.codec.http.DefaultCookie


object CookieSetSpec extends Specification {

  "CookieSet" should {
    "no cookies" in {
      val request = Request()
      request.cookies must beEmpty
    }

    "request cookie basics" in {
      val request = Request()
      request.headers("Cookie") = "name=value; name2=value2"
      request.cookies.contains(new DefaultCookie("name", "value"))   must beTrue
      request.cookies.contains(new DefaultCookie("name2", "value2")) must beTrue
      request.cookies.isValid must beTrue
    }

    "response cookie basics" in {
      val response = Response()
      response.headers("Set-Cookie") = "name=value; name2=value2"
      response.cookies.contains(new DefaultCookie("name", "value"))   must beTrue
      response.cookies.contains(new DefaultCookie("name2", "value2")) must beTrue
    }

    "cookie with attributes" in {
      val request = Request()
      request.headers("Cookie") = "name=value; Max-Age=23; Domain=.example.com; Path=/"
      val cookie = request.cookies.iterator.toList.head
      cookie.getValue    must_== "value"
      cookie.getMaxAge() must_== 23
      cookie.getDomain() must_== ".example.com"
      cookie.getPath()   must_== "/"
    }

    "add cookie" in {
      val request = Request()
      val cookie = new DefaultCookie("name", "value")
      request.cookies += cookie
      request.cookies.contains(new DefaultCookie("name", "value")) must beTrue
      request.headers("Cookie") must_== "name=value"
    }

    "remove cookie" in {
      val request = Request()
      request.headers.add("Cookie", "name=value")
      request.headers.add("Cookie", "name=value2") // same name - gets removed too

      request.cookies -= (new DefaultCookie("name", "value2"))
      request.cookies must haveSize(0)
    }

    "netty Cookie.equals is broken" in {
      val cookie1 = new DefaultCookie("name", "value")
      val cookie2 = new DefaultCookie("name", "value")
      cookie1 must_!=(cookie2)
    }

    "invalid cookies are ignored" in {
      val request = Request()
      request.headers.add("Cookie", "namé=value")
      request.cookies must haveSize(0)
      request.cookies.isValid must beFalse
    }
  }
}
