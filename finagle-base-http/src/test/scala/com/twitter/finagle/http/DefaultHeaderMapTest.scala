package com.twitter.finagle.http

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class DefaultHeaderMapTest extends AbstractHeaderMapTest with GeneratorDrivenPropertyChecks {

  import Rfc7230HeaderValidationTest._

  def genValidFoldedHeader: Gen[(String, String)] =
    for {
      k <- genNonEmptyString
      v <- genFoldedValue
    } yield (k, v)

  def genInvalidHeaderNameHeader: Gen[(String, String)] =
    for {
      (k, v) <- genValidHeader
      c <- Gen.oneOf(Seq[Char]('\t', '\n', '\f', '\r', ' ', ',', ':', ';', '=', 0x0b))
    } yield (k + c, v)

  def genNonAsciiHeaderNameHeader: Gen[(String, String)] =
    for {
      k <- genNonAsciiHeaderName
      v <- genNonEmptyString
    } yield (k, v)

  def genInvalidHeaderValueHeader: Gen[(String, String)] =
    for {
      (k, v) <- genValidHeader
      c <- Gen.oneOf(Seq[Char]('\f', 0x0b))
    } yield (k, v + c)

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
    forAll(genValidFoldedHeader) {
      case (k, v) =>
        val value = DefaultHeaderMap(k -> v).apply(k)
        assert(value == Rfc7230HeaderValidation.replaceObsFold(v))
        assert(v.contains("\n"))
        assert(!value.contains("\n"))
    }
  }

  test("validates header names (failure)") {
    forAll(genInvalidHeaderNameHeader) { h =>
      val e = intercept[IllegalArgumentException](DefaultHeaderMap(h))
      assert(e.getMessage.contains("prohibited characters"))
    }

    forAll(genNonAsciiHeaderNameHeader) { h =>
      val e = intercept[IllegalArgumentException](DefaultHeaderMap(h))
      assert(e.getMessage.contains("non-ASCII characters"))
    }
  }

  test("validates header values (failure)") {
    forAll(genInvalidHeaderValueHeader) { h =>
      val e = intercept[IllegalArgumentException](DefaultHeaderMap(h))
      assert(e.getMessage.contains("prohibited character"))
    }

    forAll(genInvalidClrfHeaderValue) { h =>
      intercept[IllegalArgumentException](DefaultHeaderMap("foo" -> h))
    }
  }

  test("does not validate header names or values with addUnsafe") {
    val headerMap = newHeaderMap()

    forAll(genInvalidHeaderNameHeader) { h =>
      headerMap.addUnsafe(h._1, h._2)
    }

    forAll(genInvalidHeaderValueHeader) { h =>
      headerMap.addUnsafe(h._1, h._2)
    }
  }

  test("does not validate header names or values with setUnsafe") {
    val headerMap = newHeaderMap()

    forAll(genInvalidHeaderNameHeader) { h =>
      headerMap.setUnsafe(h._1, h._2)
    }

    forAll(genInvalidHeaderValueHeader) { h =>
      headerMap.setUnsafe(h._1, h._2)
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
}
