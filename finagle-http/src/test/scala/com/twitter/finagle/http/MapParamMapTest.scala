package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MapParamMapTest extends FunSuite {

  test("get") {
    assert(MapParamMap().get("key") == None)
    assert(MapParamMap("key" -> "value").get("key") == Some("value"))
  }

  test("keys") {
    val paramMap = MapParamMap("a" -> "1", "b" -> "2", "a" -> "3")
    assert(paramMap.keys.toList.sorted == List("a", "b"))
    assert(paramMap.keySet.toList.sorted == List("a", "b"))
    assert(paramMap.keysIterator.toList.sorted == List("a", "b"))
  }

  test("iterator") {
    val paramMap = MapParamMap("a" -> "1", "b" -> "2", "a" -> "3")
    assert(paramMap.iterator.toList.sorted == List(("a" -> "1"), ("a" -> "3"), ("b" -> "2")))
  }

  test("+") {
    val paramMap = MapParamMap() + ("a" -> "1") + ("b" -> "2") + ("a" -> "3")
    assert(paramMap.get("a") == Some("3"))
    assert(paramMap.get("b") == Some("2"))
    assert(paramMap.getAll("a").toList == List("3"))
  }

  test("-") {
    val paramMap = MapParamMap("a" -> "1", "b" -> "2", "a" -> "3") - "a" - "b"
    assert(paramMap.isEmpty == true)
  }
}
