package com.twitter.finagle.http.headers

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.HeaderMap
import com.twitter.util.Stopwatch
import java.util.Date
import org.scalacheck.Gen
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test battery that all `HeaderMap` types should pass.
 */
abstract class AbstractHeaderMapTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with Eventually
    with IntegrationPatience {

  def newHeaderMap(headers: (String, String)*): HeaderMap

  private[this] val date = new Date(1441322139353L)
  private[this] val formattedDate = "Thu, 03 Sep 2015 23:15:39 GMT"
  import Rfc7230HeaderValidationTest._
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
    assert(map.keySet.toSet == Set("a", "b"))

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

    assert(map.get("Host") == Some("api.twitter.com"))
    assert(map.get("HOST") == Some("api.twitter.com"))
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
    assert(map.getOrNull("HOST") == "api.twitter.com")
    assert(map.getOrNull("missing") == null)
  }

  test("getAll") {
    val map = newHeaderMap("Cookie" -> "1", "Cookie" -> "2")

    assert(map.getAll("Cookie").toList.sorted == List("1", "2"))
    assert(map.getAll("COOKIE").toList.sorted == List("1", "2"))
    assert(map.getAll("missing").toList == Nil)
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
    assert(map.keySet.toSet == Set("a", "A"))

    map.set("B", "1")
    map.add("b", "2")

    assert(map.iterator.toSet == Set("a" -> "1", "A" -> "2", "a" -> "3", "B" -> "1", "b" -> "2"))
    assert(map.keySet.toSet == Set("a", "A", "b", "B"))
  }

  def genValidHeader: Gen[(String, String)] =
    for {
      k <- genNonEmptyString
      v <- genNonEmptyString
    } yield (k, v)

  test("apply()") {
    assert(newHeaderMap().isEmpty)
  }

  test("reject out-of-bound characters in name") {
    forAll(Gen.choose[Char](128, Char.MaxValue)) { c =>
      val headerMap = newHeaderMap()
      intercept[IllegalArgumentException] {
        headerMap.set(c.toString, "valid")
      }
      intercept[IllegalArgumentException] {
        headerMap.add(c.toString, "valid")
      }
      assert(headerMap.isEmpty)
    }
  }

  test("reject out-of-bound characters in value") {
    forAll(Gen.choose[Char](256, Char.MaxValue)) { c =>
      val headerMap = newHeaderMap()
      intercept[IllegalArgumentException] {
        headerMap.set("valid", c.toString)
      }
      intercept[IllegalArgumentException] {
        headerMap.add("valid", c.toString)
      }
      assert(headerMap.isEmpty)
    }
  }

  test("validates header names & values (success)") {
    forAll(genValidHeader) {
      case (k, v) =>
        assert(newHeaderMap(k -> v).get(k).contains(v))
    }
  }

  test("validates header names & values with obs-folds (success)") {
    forAll(genFoldedValue) { v =>
      val value = newHeaderMap("foo" -> v).apply("foo")
      assert(value == HeaderMap.ObsFoldRegex.replaceAllIn(v, " "))
      assert(v.contains("\n"))
      assert(!value.contains("\n"))
    }
  }

  test("validates header names (failure)") {
    forAll(genInvalidHeaderName) { k =>
      val e = intercept[IllegalArgumentException](newHeaderMap(k -> "foo"))
      assert(e.getMessage.contains("prohibited character"))
    }

    forAll(genNonAsciiHeaderName) { k =>
      val e = intercept[IllegalArgumentException](newHeaderMap(k -> "foo"))
      assert(e.getMessage.contains("prohibited character"))
    }
  }

  test("validates header values (failure)") {
    forAll(genInvalidHeaderValue) { v =>
      val e = intercept[IllegalArgumentException](newHeaderMap("foo" -> v))
      assert(e.getMessage.contains("prohibited character"))
    }

    forAll(genInvalidClrfHeaderValue) { v =>
      intercept[IllegalArgumentException](newHeaderMap("foo" -> v))
    }
  }

  test("does not validate header names or values with addUnsafe") {
    val headerMap = newHeaderMap()

    forAll(genInvalidHeaderName) { k => headerMap.addUnsafe(k, "foo") }

    forAll(genInvalidHeaderValue) { v => headerMap.addUnsafe("foo", v) }
  }

  test("does not validate header names or values with setUnsafe") {
    val headerMap = newHeaderMap()

    forAll(genInvalidHeaderName) { k => headerMap.setUnsafe(k, "foo") }

    forAll(genInvalidHeaderValue) { v => headerMap.setUnsafe("foo", v) }
  }

  test("getOrNull acts as get().orNull") {
    forAll(genValidHeader) {
      case (k, v) =>
        val h = newHeaderMap(k -> v)
        assert(h.getOrNull(k) == h.get(k).orNull)
    }

    val empty = newHeaderMap()
    assert(empty.getOrNull("foo") == empty.get("foo").orNull)
  }

  // a handful of non-property based tests
  test("empty header name is rejected") {
    intercept[IllegalArgumentException](newHeaderMap("" -> "bar"))
  }

  test("header names with separators are rejected") {
    ((0x1 to 0x20).map(_.toChar) ++ "\"(),/:;<=>?@[\\]{}").foreach { illegalChar =>
      intercept[IllegalArgumentException](newHeaderMap(illegalChar.toString -> "bar"))
    }
  }

  test("large number of collisions isn't super slow to add") {
    val num = 300 * 1000
    eventually {
      val stopwatch = Stopwatch.start()

      val hs = newHeaderMap()

      (0 until num).foreach { _ => hs.add("key", "value") }
      assert(hs.size == num)

      assert(stopwatch() < 10.seconds)
    }
  }

  test("large number of colliding names doesn't make the `.names` iterator slow") {
    def collidingNames(n: Int): Iterator[String] = new Iterator[String] {
      private[this] val stringLen: Int = (math.log(n) / math.log(2)).asInstanceOf[Int] + 1
      private[this] var i = 0

      def hasNext: Boolean = i < n
      def next(): String = {
        val s = new StringBuilder
        def go(j: Int): Unit = {
          if (j != stringLen) {
            val c = if (((1 << j) & i) == 0) 'a' else 'A'
            s.append(c)
            go(j + 1)
          }
        }
        go(0)
        i += 1
        s.result()
      }
    }

    val num = 300 * 1000
    val collisions = collidingNames(num)
    val hs = newHeaderMap()

    while (collisions.hasNext) {
      hs.add(collisions.next(), "")
    }

    // Now iterate through the keys. Took ~14 minutes before the patch.
    // Takes ~130 ms after the patch.
    eventually {
      val stopwatch = Stopwatch.start()
      var acc = 0
      // Just do some arbitrary work
      hs.keysIterator.foreach(_ => acc += 1)
      assert(acc == num)
      assert(stopwatch() < 30.seconds)

    }
  }
}
