package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HeaderMapTest extends FunSuite {
  test("get") {
    val request = Request()
    request.headers.add("Host", "api.twitter.com")

    assert(request.headerMap.get("Host")    === Some("api.twitter.com"))
    assert(request.headerMap.get("HOST")    === Some("api.twitter.com"))
    assert(request.headerMap.get("missing") === None)
  }

  test("getAll") {
    val request = Request()
    request.headers.add("Cookie", "1")
    request.headers.add("Cookie", "2")

    assert(request.headerMap.getAll("Cookie").toList.sorted === List("1", "2"))
    assert(request.headerMap.getAll("COOKIE").toList.sorted === List("1", "2"))
    assert(request.headerMap.getAll("missing").toList       === Nil)
  }

  test("iterator") {
    val request = Request()
    request.headers.add("Cookie", "1")
    request.headers.add("Cookie", "2")

    assert(request.headerMap.iterator.toList.sorted === ("Cookie", "1") :: ("Cookie", "2") :: Nil)
  }

  test("keys") {
    val request = Request()
    request.headers.add("Cookie", "1")
    request.headers.add("Cookie", "2")

    assert(request.headerMap.keys.toList === List("Cookie"))
    assert(request.headerMap.keySet.toList === List("Cookie"))
    assert(request.headerMap.keysIterator.toList === List("Cookie"))
  }

  test("contains") {
    val request = Request()
    request.headers.add("Cookie", "1")

    assert(request.headerMap.contains("Cookie") === true)
    assert(request.headerMap.contains("COOKIE") === true)
    assert(request.headerMap.contains("missing") === false)
  }

  test("add") {
    val request = Request()
    request.headers.add("Cookie", "1")

    request.headerMap.add("Cookie", "2")
    assert(request.headerMap.getAll("Cookie").toList.sorted === List("1", "2"))
  }

  test("+=") {
    val request = Request()
    request.headers.add("Cookie", "1")

    request.headerMap += "Cookie" -> "2"
    assert(request.headerMap.getAll("Cookie").toList.sorted === List("2"))
  }

  test("-=") {
    val request = Request()
    request.headers.add("Cookie", "1")

    request.headerMap -= "Cookie"
    assert(request.headerMap.contains("Cookie") === false)
  }
}
