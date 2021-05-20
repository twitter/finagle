package com.twitter.finagle.http.headers

import com.twitter.finagle.http.headers.Rfc7230HeaderValidation.{ObsFoldDetected, ValidationFailure}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

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
      (0x21 to 0x7e).toSet -- // VCHAR = 0x21-0x7E
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

class Rfc7230HeaderValidationTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {
  import Rfc7230HeaderValidationTest._

  private[this] def assertFailure(expectedMsgFragment: String)(f: => Any): Unit = {
    f match {
      case ValidationFailure(e) =>
        assert(e.getMessage.contains(expectedMsgFragment))

      case other => fail(s"Unexpected result: $other")
    }
  }

  private[this] def assertProhibited(f: Any): Unit =
    assertFailure("prohibited character")(f)

  test("reject out-of-bound characters in name") {
    forAll(Gen.choose[Char](128, Char.MaxValue)) { c =>
      assertProhibited(Rfc7230HeaderValidation.validateName(c.toString))
    }
  }

  test("reject out-of-bound characters in value") {
    forAll(Gen.choose[Char](256, Char.MaxValue)) { c =>
      assertProhibited(Rfc7230HeaderValidation.validateValue("foo", c.toString))
    }
  }

  test("detects values with obs-folds (success)") {
    forAll(genFoldedValue) { v =>
      assert(Rfc7230HeaderValidation.validateValue("foo", v) == ObsFoldDetected)
    }
  }

  test("validates header names (failure)") {
    forAll(genInvalidHeaderName) { h => assertProhibited(Rfc7230HeaderValidation.validateName(h)) }

    forAll(genNonAsciiHeaderName) { h => assertProhibited(Rfc7230HeaderValidation.validateName(h)) }
  }

  test("validates header values (failure)") {
    forAll(genInvalidHeaderValue) { h =>
      assertProhibited(Rfc7230HeaderValidation.validateValue("foo", h))
    }

    forAll(genInvalidClrfHeaderValue) { h =>
      assertFailure("")(Rfc7230HeaderValidation.validateValue("foo", h))
    }
  }

  test("line feeds are prohibited in header names") {
    val invalid = for {
      sep <- Seq("\n", "\r\n")
      first <- Seq("", "foo")
      second <- Seq("", "bar")
    } yield (first + sep + second)

    invalid.foreach { key => assertProhibited(Rfc7230HeaderValidation.validateName(key)) }
  }

  // The following two tests are non-RFC compliant but we see them enough
  // and they are benign enough that we accept them.
  test("null char (0x0) is considered invalid in names") {
    assertProhibited(Rfc7230HeaderValidation.validateName("\u0000foo"))
  }

  test("null char (0x0) is considered invalid in values") {
    val e =
      assertProhibited(Rfc7230HeaderValidation.validateValue("foo", "\u0000bar"))
  }
}
