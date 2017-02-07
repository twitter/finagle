package com.twitter.finagle.http

class MapHeaderMapTest extends AbstractHeaderMapTest {

  final def newHeaderMap(headers: (String, String)*): HeaderMap = MapHeaderMap(headers:_*)

  test("apply()") {
    assert(MapHeaderMap().size == 0)
    assert(MapHeaderMap().isEmpty)
  }

  test("apply(kv1, kv2) is the same as adding headers") {
    val testHeaders = Seq("a" -> "a1", "A" -> "a2", "b" -> "b")
    val map1 = MapHeaderMap(testHeaders:_*)

    val map2 = {
      val m = MapHeaderMap()
      testHeaders.foreach { case (k, v) => m.add(k, v) }
      m
    }

    assert(map1.get("a") == Some("a1"))
    assert(map1.get("a") == map2.get("a"))

    assert(map1.get("A") == Some("a1"))
    assert(map1.get("A") == map2.get("A"))

    assert(map1.get("b") == Some("b"))
    assert(map1.get("b") == map2.get("b"))

    assert(map1.getAll("a") == Seq("a1", "a2"))
    assert(map1.getAll("A") == map1.getAll("A"))

    assert(map1.getAll("A") == Seq("a1", "a2"))
    assert(map1.getAll("A") == map1.getAll("A"))


    assert(map1.iterator.toSeq.sorted == Seq("A" -> "a2", "a" -> "a1", "b" -> "b"))
    assert(map1.iterator.toSeq.sorted == map2.iterator.toSeq.sorted)
  }
}
