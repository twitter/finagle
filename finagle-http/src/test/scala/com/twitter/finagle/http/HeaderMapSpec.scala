package com.twitter.finagle.http

import org.specs.SpecificationWithJUnit


class HeaderMapSpec extends SpecificationWithJUnit {
  "MessageHeaderMap" should {
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
      request.headers.keySet.toList must_== "Cookie" :: Nil
      request.headers.keysIterator.toList must_== "Cookie" :: Nil
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

  "MapHeaderMap" should {
    "empty map" in {
      val map = MapHeaderMap()
      map.get("key") must beNone
      map.getAll("key") must beEmpty
      map.iterator must beEmpty
    }

    "map basics" in {
      val map = MapHeaderMap("a" -> "1", "b" -> "2", "a" -> "3")

      map.get("a") must beSome("1")
      map.getAll("a").toList must_== "1" :: "3" :: Nil
      map.iterator.toList.sorted must_== ("a" -> "1") :: ("a" -> "3") :: ("b" -> "2") :: Nil

      map.keys.toList.sorted must_== "a" :: "b" :: Nil
      map.keySet.toList.sorted must_== "a" :: "b" :: Nil
      map.keysIterator.toList.sorted must_== "a" :: "b" :: Nil
    }

    "+=" in {
      val map = MapHeaderMap()
      map += "a" -> "1"
      map += "b" -> "2"
      map += "a" -> "3"
      map.get("a") must beSome("3")
      map.getAll("a").toList must_== "3" :: Nil
      map.iterator.toList.sorted must_== ("a" -> "3") :: ("b" -> "2") :: Nil
    }

    "add" in {
      val map = MapHeaderMap()
      map.add("a", "1")
      map.add("b", "2")
      map.add("a", "3")
      map.get("a") must beSome("1")
      map.getAll("a").toList must_== "1" :: "3" :: Nil
      map.iterator.toList.sorted must_== ("a" -> "1") :: ("a" -> "3") :: ("b" -> "2") :: Nil
    }

    "-=" in {
      val map = MapHeaderMap("a" -> "1", "b" -> "2", "a" -> "3")
      map -= "a"
      map -= "b"
      map.get("a") must beNone
      map.getAll("a") must beEmpty
      map.iterator must beEmpty
      map -= "a" // this is legal
    }
  }
}
