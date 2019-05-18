package com.twitter.finagle.http

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class DefaultHeaderMapTest extends AbstractHeaderMapTest with GeneratorDrivenPropertyChecks {

  import Rfc7230HeaderValidationTest._

  def genValidHeader: Gen[(String, String)] =
    for {
      k <- genNonEmptyString
      v <- genNonEmptyString
    } yield (k, v)

  final def newHeaderMap(headers: (String, String)*): HeaderMap = DefaultHeaderMap(headers: _*)

  test("apply()") {
    assert(DefaultHeaderMap().isEmpty)
  }

  test("reject out-of-bound characters in name") {
    forAll(Gen.choose[Char](128, Char.MaxValue)) { c =>
      val headerMap = DefaultHeaderMap()
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
      val headerMap = DefaultHeaderMap()
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
        assert(DefaultHeaderMap(k -> v).get(k).contains(v))
    }
  }

  test("validates header names & values with obs-folds (success)") {
    forAll(genFoldedValue) { v =>
      val value = DefaultHeaderMap("foo" -> v).apply("foo")
      assert(value == DefaultHeaderMap.ObsFoldRegex.replaceAllIn(v, " "))
      assert(v.contains("\n"))
      assert(!value.contains("\n"))
    }
  }

  test("validates header names (failure)") {
    forAll(genInvalidHeaderName) { k =>
      val e = intercept[IllegalArgumentException](DefaultHeaderMap(k -> "foo"))
      assert(e.getMessage.contains("prohibited character"))
    }

    forAll(genNonAsciiHeaderName) { k =>
      val e = intercept[IllegalArgumentException](DefaultHeaderMap(k -> "foo"))
      assert(e.getMessage.contains("prohibited character"))
    }
  }

  test("validates header values (failure)") {
    forAll(genInvalidHeaderValue) { v =>
      val e = intercept[IllegalArgumentException](DefaultHeaderMap("foo" -> v))
      assert(e.getMessage.contains("prohibited character"))
    }

    forAll(genInvalidClrfHeaderValue) { v =>
      intercept[IllegalArgumentException](DefaultHeaderMap("foo" -> v))
    }
  }

  test("does not validate header names or values with addUnsafe") {
    val headerMap = newHeaderMap()

    forAll(genInvalidHeaderName) { k =>
      headerMap.addUnsafe(k, "foo")
    }

    forAll(genInvalidHeaderValue) { v =>
      headerMap.addUnsafe("foo", v)
    }
  }

  test("does not validate header names or values with setUnsafe") {
    val headerMap = newHeaderMap()

    forAll(genInvalidHeaderName) { k =>
      headerMap.setUnsafe(k, "foo")
    }

    forAll(genInvalidHeaderValue) { v =>
      headerMap.setUnsafe("foo", v)
    }
  }

  test("getOrNull acts as get().orNull") {
    forAll(genValidHeader) {
      case (k, v) =>
        val h = DefaultHeaderMap(k -> v)
        assert(h.getOrNull(k) == h.get(k).orNull)
    }

    val empty = DefaultHeaderMap()
    assert(empty.getOrNull("foo") == empty.get("foo").orNull)
  }

  // a handful of non-property based tests
  test("empty header name is rejected") {
    intercept[IllegalArgumentException](DefaultHeaderMap("" -> "bar"))
  }

  test("header names with separators are rejected") {
    ((0x1 to 0x20).map(_.toChar) ++ "\"(),/:;<=>?@[\\]{}").foreach { illegalChar =>
      intercept[IllegalArgumentException](DefaultHeaderMap(illegalChar.toString -> "bar"))
    }
  }
}
