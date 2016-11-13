package com.twitter.finagle.http

import java.util.Date
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HeaderMapTest extends FunSuite {

  private[this] val date = new Date(1441322139353L)
  private[this] val formattedDate = "Thu, 03 Sep 2015 23:15:39 GMT"

  test("get") {
    val request = Request()
    request.headers.add("Host", "api.twitter.com")

    assert(request.headerMap.get("Host")    == Some("api.twitter.com"))
    assert(request.headerMap.get("HOST")    == Some("api.twitter.com"))
    assert(request.headerMap.get("missing") == None)
  }

  test("set") {
    val request = Request()
    request.headers.set("key", "initial value")
    assert(request.headerMap.get("key") == Some("initial value"))

    request.headers.set("key", "replacement value")
    assert(request.headerMap.get("key") == Some("replacement value"))
    assert(request.headerMap.getAll("key").toList == List("replacement value"))

    request.headerMap.set("date", date)
    assert(request.headerMap.getAll("date").toSeq == Seq(formattedDate))
  }

  test("getAll") {
    val request = Request()
    request.headers.add("Cookie", "1")
    request.headers.add("Cookie", "2")

    assert(request.headerMap.getAll("Cookie").toList.sorted == List("1", "2"))
    assert(request.headerMap.getAll("COOKIE").toList.sorted == List("1", "2"))
    assert(request.headerMap.getAll("missing").toList       == Nil)
  }

  test("iterator") {
    val request = Request()
    request.headers.add("Cookie", "1")
    request.headers.add("Cookie", "2")

    assert(request.headerMap.iterator.toList.sorted == ("Cookie", "1") :: ("Cookie", "2") :: Nil)
  }

  test("keys") {
    val request = Request()
    request.headers.add("Cookie", "1")
    request.headers.add("Cookie", "2")

    assert(request.headerMap.keys.toList == List("Cookie"))
    assert(request.headerMap.keySet.toList == List("Cookie"))
    assert(request.headerMap.keysIterator.toList == List("Cookie"))
  }

  test("contains") {
    val request = Request()
    request.headers.add("Cookie", "1")

    assert(request.headerMap.contains("Cookie") == true)
    assert(request.headerMap.contains("COOKIE") == true)
    assert(request.headerMap.contains("missing") == false)
  }

  test("add") {
    val request = Request()
    request.headers.add("Cookie", "1")

    request.headerMap.add("Cookie", "2")
    assert(request.headerMap.getAll("Cookie").toList.sorted == List("1", "2"))

    request.headerMap.add("date", date)
    assert(request.headerMap.getAll("date").toSeq == Seq(formattedDate))
  }

  test("+=") {
    val request = Request()
    request.headers.add("Cookie", "1")

    request.headerMap += "Cookie" -> "2"
    assert(request.headerMap.getAll("Cookie").toList.sorted == List("2"))

    request.headerMap += "date" -> date
    assert(request.headerMap.getAll("date").toSeq == Seq(formattedDate))
  }

  test("-=") {
    val request = Request()
    request.headers.add("Cookie", "1")

    request.headerMap -= "Cookie"
    assert(request.headerMap.contains("Cookie") == false)
  }
}
