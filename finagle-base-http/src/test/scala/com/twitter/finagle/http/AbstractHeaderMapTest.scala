package com.twitter.finagle.http

import java.util.Date
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Test battery that all `HeaderMap` types should pass.
 */
@RunWith(classOf[JUnitRunner])
abstract class AbstractHeaderMapTest extends FunSuite {

  // Constructs a new HeaderMap with the semantics of headers.foreach { case (k,v) => map.add(k,v) }
  def newHeaderMap(headers: (String, String)*): HeaderMap

  private[this] val date = new Date(1441322139353L)
  private[this] val formattedDate = "Thu, 03 Sep 2015 23:15:39 GMT"


  ////// MapHeaderMap derived tests /////////////

  test("empty map") {
    val map = newHeaderMap()
    assert(map.get("key") == None)
    assert(map.getOrNull("key") == null)
    assert(map.getAll("key").isEmpty == true)
    assert(map.iterator.isEmpty == true)
  }

  test("map basics") {
    val map = newHeaderMap("a" -> "1", "b" -> "2", "a" -> "3")

    assert(map.get("a") == Some("1"))
    assert(map.get("missing") == None)

    assert(map.getOrNull("a") == "1")
    assert(map.getOrNull("missing") == null)

    assert(map.getAll("a").toSet == Set("1", "3"))
    assert(map.getAll("missing").toSet == Set())

    assert(map.iterator.toSet == Set(("a" -> "1"), ("a" -> "3"), ("b" -> "2")))

    assert(map.keys.toSet == Set("a", "b"))
    assert(map.keySet == Set("a", "b"))

    assert(map.keysIterator.toSet == Set("a", "b"))
  }

  test("contains") {
    val map = newHeaderMap("Cookie" -> "1")

    assert(map.contains("Cookie") == true)
    assert(map.contains("COOKIE") == true)
    assert(map.contains("missing") == false)
  }

  test("set") {
    val map = newHeaderMap()
    map.set("a", "1")
    assert(map.get("a") == Some("1"))
    map.set("b", "2")
    map.set("a", "3")

    assert(map.get("a") == Some("3"))
    assert(map.getAll("a").toSet == Set("3"))
    assert(map.iterator.toSet == Set(("a" -> "3"), ("b" -> "2")))

    map.set("date", date)
    assert(map.getAll("date").toSeq == Seq(formattedDate))
  }

  test("set is case insensitive to the key") {
    val map = newHeaderMap("a" -> "1", "a" -> "3", "A" -> "2")
    map.set("a", "4")

    assert(map.iterator.toSet == Set("a" -> "4"))
  }

  test("+= is a synonym for set") {
    // Note: these tests should exactly mirror the "set" tests above
    val map = newHeaderMap()
    map += "a" -> "1"
    assert(map.get("a") == Some("1"))
    map += "b" -> "2"
    map += "a" -> "3"

    assert(map.get("a") == Some("3"))
    assert(map.getAll("a").toSet == Set("3"))
    assert(map.iterator.toSet == Set(("a" -> "3"), ("b" -> "2")))

    map += "date" -> date
    assert(map.getAll("date") == Seq(formattedDate))
  }

  test("+= is case insensitive to the key") {
    val map = newHeaderMap("a" -> "1", "a" -> "3", "A" -> "2")
    map += ("a" -> "4")

    assert(map.iterator.toSet == Set("a" -> "4"))
  }

  test("add") {
    val map = newHeaderMap()
    map.add("a", "1")
    map.add("b", "2")
    map.add("a", "3")

    assert(map.get("a") == Some("1"))
    assert(map.getAll("a").toSet == Set("1", "3"))
    assert(map.iterator.toSet == Set(("a" -> "1"), ("a" -> "3"), ("b" -> "2")))

    map.add("date", date)
    assert(map.getAll("date") == Seq(formattedDate))
  }

  test("-=") {
    val map = newHeaderMap("a" -> "1", "b" -> "2", "a" -> "3")
    map -= "a"
    map -= "b"

    assert(map.get("a") == None)
    assert(map.getAll("a").isEmpty == true)
    assert(map.iterator.isEmpty == true)
    map -= "a" // this is legal, even if the value is missing
  }

  test("-= is case insensitive") {
    val map = newHeaderMap()

    map += ("a" -> "5")
    map -= "A"
    assert(map.keySet.isEmpty == true)

    map += ("A" -> "5")
    map -= "a"
    assert(map.keySet.isEmpty == true)
  }

  test("get") {
    val map = newHeaderMap("Host" -> "api.twitter.com")

    assert(map.get("Host")    == Some("api.twitter.com"))
    assert(map.get("HOST")    == Some("api.twitter.com"))
    assert(map.get("missing") == None)
  }


  test("get is case insensitive and returns the first inserted header") {
    val map1 = newHeaderMap()
    map1.set("a", "1")
    map1.add("A", "2")
    assert(map1.get("a") == Some("1"))
    assert(map1.get("A") == Some("1"))

    val map2 = newHeaderMap()
    map2.set("A", "5")
    map2.add("a", "6")
    map2.add("a", "7")
    assert(map2.get("a") == Some("5"))
    assert(map2.get("A") == Some("5"))
  }

  test("getOrNull") {
    val map = newHeaderMap("Host" -> "api.twitter.com")

    assert(map.getOrNull("Host") == "api.twitter.com")
    assert(map.getOrNull("HOST")    == "api.twitter.com")
    assert(map.getOrNull("missing") == null)
  }

  test("getAll") {
    val map = newHeaderMap("Cookie" -> "1", "Cookie" -> "2")

    assert(map.getAll("Cookie").toList.sorted == List("1", "2"))
    assert(map.getAll("COOKIE").toList.sorted == List("1", "2"))
    assert(map.getAll("missing").toList       == Nil)
  }

  test("getAll is case insensitive") {
    val map = newHeaderMap()

    map.set("a", "1")
    map.add("a", "3")
    map.add("A", "4")
    assert(map.getAll("a").toSet == Set("1", "3", "4"))
    assert(map.getAll("A").toSet == Set("1", "3", "4"))
  }

  test("keys") {
    val map = newHeaderMap("Cookie" -> "1", "Cookie" -> "2")

    assert(map.keys.toList == Seq("Cookie"))
    assert(map.keySet.toList == Seq("Cookie"))
    assert(map.keysIterator.toList == Seq("Cookie"))
  }

  test("iterator") {
    val map = newHeaderMap("Cookie" -> "1", "Cookie" -> "2")
    assert(map.iterator.toList.sorted == ("Cookie", "1") :: ("Cookie", "2") :: Nil)
  }

  test("keysIterator") {
    val map = newHeaderMap("a" -> "a1", "b" -> "b", "A" -> "A", "a" -> "a2")
    assert(map.keysIterator.toList.sorted == List("A", "a", "b"))
  }

  test("iterator and keySet exposes original header names") {
    val map = newHeaderMap("a" -> "1", "A" -> "2", "a" -> "3")

    assert(map.iterator.toSet == Set("a" -> "1", "A" -> "2", "a" -> "3"))
    assert(map.keySet == Set("a", "A"))

    map.set("B", "1")
    map.add("b", "2")

    assert(map.iterator.toSet == Set("a" -> "1", "A" -> "2", "a" -> "3", "B" -> "1", "b" -> "2"))
    assert(map.keySet == Set("a", "A", "b", "B"))
  }
}
