package com.twitter.finagle.http

import org.specs.SpecificationWithJUnit


class HeaderMapSpec extends SpecificationWithJUnit {
  "MessageHeaderMap" should {
    "get" in {
      val request = Request()
      request.headers.add("Host", "api.twitter.com")

      request.headerMap.get("Host")    must_== Some("api.twitter.com")
      request.headerMap.get("HOST")    must_== Some("api.twitter.com")
      request.headerMap.get("missing") must_== None
    }

    "getAll" in {
      val request = Request()
      request.headers.add("Cookie", "1")
      request.headers.add("Cookie", "2")

      request.headerMap.getAll("Cookie").toList.sorted must_== "1" :: "2" :: Nil
      request.headerMap.getAll("COOKIE").toList.sorted must_== "1" :: "2" :: Nil
      request.headerMap.getAll("missing").toList       must_== Nil
    }

    "iterator" in {
      val request = Request()
      request.headers.add("Cookie", "1")
      request.headers.add("Cookie", "2")

      request.headerMap.iterator.toList.sorted must_== ("Cookie", "1") :: ("Cookie", "2") :: Nil
    }

    "keys" in {
      val request = Request()
      request.headers.add("Cookie", "1")
      request.headers.add("Cookie", "2")

      request.headerMap.keys.toList must_== "Cookie" :: Nil
      request.headerMap.keySet.toList must_== "Cookie" :: Nil
      request.headerMap.keysIterator.toList must_== "Cookie" :: Nil
    }

    "contains" in {
      val request = Request()
      request.headers.add("Cookie", "1")

      request.headerMap.contains("Cookie")  must beTrue
      request.headerMap.contains("COOKIE")  must beTrue
      request.headerMap.contains("missing") must beFalse
    }

    "add" in {
      val request = Request()
      request.headers.add("Cookie", "1")

      request.headerMap.add("Cookie", "2")
      request.headerMap.getAll("Cookie").toList.sorted must_== "1" :: "2" :: Nil
    }

    "+=" in {
      val request = Request()
      request.headers.add("Cookie", "1")

      request.headerMap += "Cookie" -> "2"
      request.headerMap.getAll("Cookie").toList.sorted must_== "2" :: Nil
    }

    "-=" in {
      val request = Request()
      request.headers.add("Cookie", "1")

      request.headerMap -= "Cookie"
      request.headerMap.contains("Cookie") must beFalse
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
