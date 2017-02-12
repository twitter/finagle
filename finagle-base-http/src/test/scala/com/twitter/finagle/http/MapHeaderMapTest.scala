package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MapHeaderMapTest extends FunSuite {

  test("empty map") {
    val map = MapHeaderMap()
    assert(map.get("key") == None)
    assert(map.getOrNull("key") == null)
    assert(map.getAll("key").isEmpty == true)
    assert(map.iterator.isEmpty == true)
  }

  test("map basics") {
    val map = MapHeaderMap("a" -> "1", "b" -> "2", "a" -> "3")

    assert(map.get("a") == Some("1"))
    assert(map.getOrNull("a") == "1")
    assert(map.getAll("a").toSet == Set("1", "3"))
    assert(map.iterator.toSet == Set(("a" -> "1"), ("a" -> "3"), ("b" -> "2")))

    assert(map.keys.toSet == Set("a", "b"))
    assert(map.keySet == Set("a", "b"))
    assert(map.keysIterator.toSet == Set("a", "b"))
  }

  test("+=") {
    val map = MapHeaderMap()
    map += "a" -> "1"
    map += "b" -> "2"
    map += "a" -> "3"

    assert(map.get("a") == Some("3"))
    assert(map.getAll("a").toSet == Set("3"))
    assert(map.iterator.toSet == Set(("a" -> "3"), ("b" -> "2")))
  }

  test("add") {
    val map = MapHeaderMap()
    map.add("a", "1")
    map.add("b", "2")
    map.add("a", "3")

    assert(map.get("a") == Some("1"))
    assert(map.getAll("a").toSet == Set("1", "3"))
    assert(map.iterator.toSet == Set(("a" -> "1"), ("a" -> "3"), ("b" -> "2")))
  }

  test("set") {
    val map = MapHeaderMap()
    map.set("a", "1")
    assert(map.get("a") == Some("1"))
    map.set("b", "2")
    map.set("a", "3")

    assert(map.get("a") == Some("3"))
    assert(map.getAll("a").toSet == Set("3"))
    assert(map.iterator.toSet == Set(("a" -> "3"), ("b" -> "2")))
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

  test("get is case insensitive and returns the first inserted header") {
    val map1 = MapHeaderMap()
    map1.set("a", "1")
    map1.add("A", "2")
    assert(map1.get("a") == Some("1"))
    assert(map1.get("A") == Some("1"))

    val map2 = MapHeaderMap()
    map2.set("A", "5")
    map2.add("a", "6")
    map2.add("a", "7")
    assert(map2.get("a") == Some("5"))
    assert(map2.get("A") == Some("5"))
  }

  test("getAll is case insensitive") {
    val map = MapHeaderMap()

    map.set("a", "1")
    map.add("a", "3")
    map.add("A", "4")
    assert(map.getAll("a").toSet == Set("1", "3", "4"))
    assert(map.getAll("A").toSet == Set("1", "3", "4"))
  }

  test("-= is case insensitive") {
    val map = MapHeaderMap()

    map += ("a" -> "5")
    map -= "A"
    assert(map.keySet.isEmpty == true)

    map += ("A" -> "5")
    map -= "a"
    assert(map.keySet.isEmpty == true)
  }

  test("iterator and keySet exposes original header names") {
    val map = MapHeaderMap("a" -> "1", "A" -> "2", "a" -> "3")

    assert(map.iterator.toSet == Set("a" -> "1", "A" -> "2", "a" -> "3"))
    assert(map.keySet == Set("a", "A"))

    map.set("B", "1")
    map.add("b", "2")

    assert(map.iterator.toSet == Set("a" -> "1", "A" -> "2", "a" -> "3", "B" -> "1", "b" -> "2"))
    assert(map.keySet == Set("a", "A", "b", "B"))
  }

  test("preserves the legacy behavior of +=") {
    val map = MapHeaderMap("a" -> "1", "a" -> "3", "A" -> "2")
    map += ("a" -> "4")

    assert(map.iterator.toSet == Set("a" -> "4", "A" -> "2"))
  }
}
