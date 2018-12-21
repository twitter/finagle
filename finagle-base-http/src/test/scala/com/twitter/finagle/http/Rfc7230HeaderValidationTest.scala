package com.twitter.finagle.http

import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

private object Rfc7230HeaderValidationTest {

  // Definitions:
  // VCHAR = 0x21-0x7E defined at https://tools.ietf.org/html/rfc5234#appendix-B.1
  // obs-text = 0x80-0xff defined at https://tools.ietf.org/html/rfc7230#section-3.2.6
  // Delimiters defined in https://tools.ietf.org/html/rfc7230#section-3.2.6

  val invalidNameChars: Seq[Char] = {
    // Start with all the chars from 0x0 to 0xff and exclude the valid ones.
    // We don't use the definition in Rfc7230HeaderValidation so that we don't make a
    // self consistent but wrong test.
    val delimiters = "(),/:;<=>?@[\\]{}\"".map(_.toInt).toSet

    val invalid = (0x00 to 0xff).toSet --
      (0x21 to 0x7e).toSet ++ // VCHAR
      delimiters // add back the delimiters into the invalid characters

    invalid.toSeq.map(_.toChar)
  }

  val invalidValueChars: Seq[Char] = {
    // Start with all the chars from 0x0 to 0xff and exclude the valid ones.
    // We don't use the definition in Rfc7230HeaderValidation so that we don't make a
    // self consistent but wrong test.
    val invalid = (0x0 to 0xff).toSet --
      (0x21 to 0x7E).toSet -- // VCHAR = 0x21-0x7E
      (0x80 to 0xff).toSet -- // obs-text =  0x80-0xff
      " \t\r\n".map(_.toInt).toSet // valid whitespace: " \t\r\n"

    invalid.toSeq.map(_.toChar)
  }

  def genNonEmptyString: Gen[String] =
    Gen.nonEmptyListOf(Gen.choose('a', 'z')).map(s => new String(s.toArray))

  def genFoldedValue: Gen[String] =
    for {
      v1 <- genNonEmptyString
      folds <- Gen.nonEmptyListOf(Gen.oneOf("\r\n ", "\r\n\t", "\n ", "\n\t"))
      v2 <- genNonEmptyString
    } yield (v1 + folds + v2)

  def genInvalidHeaderName: Gen[String] =
    for {
      k <- genNonEmptyString
      c <- Gen.oneOf(invalidNameChars)
    } yield k + c

  def genNonAsciiHeaderName: Gen[String] =
    for {
      k <- genNonEmptyString
      c <- Gen.choose[Char](128, Char.MaxValue)
    } yield k + c

  def genInvalidHeaderValue: Gen[String] =
    for {
      v <- genNonEmptyString
      c <- Gen.oneOf(invalidValueChars)
    } yield (v + c)

  def genInvalidClrfHeaderValue: Gen[String] =
    for {
      v <- genNonEmptyString
      c <- Gen.oneOf("\rx", "\nx", "\r", "\n")
    } yield (v + c)
}

class Rfc7230HeaderValidationTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import Rfc7230HeaderValidationTest._

  test("reject out-of-bound characters in name") {
    forAll(Gen.choose[Char](128, Char.MaxValue)) { c =>
      val e = intercept[IllegalArgumentException] {
        Rfc7230HeaderValidation.validateName(c.toString)
      }
      assert(e.getMessage.contains("prohibited character"))
    }
  }

  test("reject out-of-bound characters in value") {
    forAll(Gen.choose[Char](256, Char.MaxValue)) { c =>
      val e = intercept[IllegalArgumentException] {
        Rfc7230HeaderValidation.validateValue("foo", c.toString)
      }
      assert(e.getMessage.contains("prohibited character"))
    }
  }

  test("detects values with obs-folds (success)") {
    forAll(genFoldedValue) { v =>
      assert(Rfc7230HeaderValidation.validateValue("foo", v))
    }
  }

  test("validates header names (failure)") {
    forAll(genInvalidHeaderName) { h =>
      val e = intercept[IllegalArgumentException](Rfc7230HeaderValidation.validateName(h))
      assert(e.getMessage.contains("prohibited character"))
    }

    forAll(genNonAsciiHeaderName) { h =>
      val e = intercept[IllegalArgumentException](Rfc7230HeaderValidation.validateName(h))
      assert(e.getMessage.contains("prohibited character"))
    }
  }

  test("validates header values (failure)") {
    forAll(genInvalidHeaderValue) { h =>
      val e = intercept[IllegalArgumentException](Rfc7230HeaderValidation.validateValue("foo", h))
      assert(e.getMessage.contains("prohibited character"))
    }

    forAll(genInvalidClrfHeaderValue) { h =>
      intercept[IllegalArgumentException](Rfc7230HeaderValidation.validateValue("foo", h))
    }
  }

  // The following two tests are non-RFC compliant but we see them enough
  // and they are benign enough that we accept them.
  test("null char (0x0) is considered invalid in names") {
    val e = intercept[IllegalArgumentException](Rfc7230HeaderValidation.validateName("\u0000foo"))
    assert(e.getMessage.contains("prohibited character"))
  }

  test("null char (0x0) is considered invalid in values") {
    val e = intercept[IllegalArgumentException](Rfc7230HeaderValidation.validateValue("foo", "\u0000bar"))
    assert(e.getMessage.contains("prohibited character"))
  }
}
