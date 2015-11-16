package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MapHeaderMapTest extends FunSuite {

  test("empty map") {
    val map = MapHeaderMap()
    assert(map.get("key") == None)
    assert(map.getAll("key").isEmpty == true)
    assert(map.iterator.isEmpty == true)
  }

  test("map basics") {
    val map = MapHeaderMap("a" -> "1", "b" -> "2", "a" -> "3")

    assert(map.get("a") == Some("1"))
    assert(map.getAll("a").toList == List("1", "3"))
    assert(map.iterator.toList.sorted == List(("a" -> "1"), ("a" -> "3"), ("b" -> "2")))

    assert(map.keys.toList.sorted == List("a", "b"))
    assert(map.keySet.toList.sorted == List("a", "b"))
    assert(map.keysIterator.toList.sorted == List("a", "b"))
  }

  test("+=") {
    val map = MapHeaderMap()
    map += "a" -> "1"
    map += "b" -> "2"
    map += "a" -> "3"

    assert(map.get("a") == Some("3"))
    assert(map.getAll("a").toList == List("3"))
    assert(map.iterator.toList.sorted == List(("a" -> "3"), ("b" -> "2")))
  }

  test("add") {
    val map = MapHeaderMap()
    map.add("a", "1")
    map.add("b", "2")
    map.add("a", "3")

    assert(map.get("a") == Some("1"))
    assert(map.getAll("a").toList == List("1", "3"))
    assert(map.iterator.toList.sorted == List(("a" -> "1"), ("a" -> "3"), ("b" -> "2")))
  }

  test("set") {
    val map = MapHeaderMap()
    map.set("a", "1")
    assert(map.get("a") == Some("1"))
    map.set("b", "2")
    map.set("a", "3")

    assert(map.get("a") == Some("3"))
    assert(map.getAll("a").toList == List("3"))
    assert(map.iterator.toList.sorted == List(("a" -> "3"), ("b" -> "2")))
  }

  test("-=") {
    val map = MapHeaderMap("a" -> "1", "b" -> "2", "a" -> "3")
    map -= "a"
    map -= "b"

    assert(map.get("a") == None)
    assert(map.getAll("a").isEmpty == true)
    assert(map.iterator.isEmpty == true)
    map -= "a" // this is legal
  }
}
