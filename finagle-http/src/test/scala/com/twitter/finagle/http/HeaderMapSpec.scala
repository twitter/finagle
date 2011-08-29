package com.twitter.finagle.http

import org.specs.Specification


object HeaderMapSpec extends Specification {
  "HeaderMap" should {
    "get" in {
      val request = Request()
      request.addHeader("Host", "api.twitter.com")

      request.headers.get("Host")    must_== Some("api.twitter.com")
      request.headers.get("HOST")    must_== Some("api.twitter.com")
      request.headers.get("missing") must_== None
    }

    "getAll" in {
      val request = Request()
      request.addHeader("Cookie", "1")
      request.addHeader("Cookie", "2")

      request.headers.getAll("Cookie").toList.sorted must_== "1" :: "2" :: Nil
      request.headers.getAll("COOKIE").toList.sorted must_== "1" :: "2" :: Nil
      request.headers.getAll("missing").toList       must_== Nil
    }

    "iterator" in {
      val request = Request()
      request.addHeader("Cookie", "1")
      request.addHeader("Cookie", "2")

      request.headers.iterator.toList.sorted must_== ("Cookie", "1") :: ("Cookie", "2") :: Nil
    }

    "keys" in {
      val request = Request()
      request.addHeader("Cookie", "1")
      request.addHeader("Cookie", "2")

      request.headers.keys.toList must_== "Cookie" :: Nil
    }

    "contains" in {
      val request = Request()
      request.addHeader("Cookie", "1")

      request.headers.contains("Cookie")  must beTrue
      request.headers.contains("COOKIE")  must beTrue
      request.headers.contains("missing") must beFalse
    }

    "add" in {
      val request = Request()
      request.addHeader("Cookie", "1")

      request.headers.add("Cookie", "2")
      request.headers.getAll("Cookie").toList.sorted must_== "1" :: "2" :: Nil
    }

    "+=" in {
      val request = Request()
      request.addHeader("Cookie", "1")

      request.headers += "Cookie" -> "2"
      request.headers.getAll("Cookie").toList.sorted must_== "2" :: Nil
    }

    "-=" in {
      val request = Request()
      request.addHeader("Cookie", "1")

      request.headers -= "Cookie"
      request.headers.contains("Cookie") must beFalse
    }
  }
}
