package com.twitter.finagle.memcached.unit.util

import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf
import java.nio.charset.StandardCharsets.UTF_8
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class ParserUtilsTest extends FunSuite  with GeneratorDrivenPropertyChecks {

  private def isDigitsBB(str: String): Boolean = {
    val bb = UTF_8.encode(str)
    ParserUtils.isDigits(Buf.ByteBuffer.Owned(bb))
  }

  private def isDigitsBA(str: String): Boolean = {
    val bytes = str.getBytes(UTF_8)
    ParserUtils.isDigits(Buf.ByteArray.Owned(bytes))
  }

  val strings =
    Seq(
      "123" -> true,
      "1"   -> true,
      ""    -> false,
      " "   -> false,
      "x"   -> false,
      " 9"  -> false,
      "9 "  -> false
    )

  test("isDigits: Buf.ByteBuffer") {
    strings.foreach { case (str, expected) =>
      withClue(str) {
        assert(isDigitsBB(str) == expected)
      }
    }
  }

  test("isDigits: Buf.ByteArray") {
    strings.foreach { case (str, expected) =>
      withClue(str) {
        assert(isDigitsBA(str) == expected)
      }
    }
  }

  test("ByteArrayString to positive Int") {
    forAll { num: Int =>
      val bytes = num.toString.getBytes(UTF_8)
      if (num >= 0)
        assert(ParserUtils.byteArrayStringToInt(bytes, bytes.length) == num)
      else
        assert(ParserUtils.byteArrayStringToInt(bytes, bytes.length) == -1)
    }

    // check cases where the byte array is an invalid Int String or length is invalid
    val stringsAndLengths =
      Seq(
        ("xxxx", -50),
        ("xxxx", 1),
        ("123", -50),
        ("123", 4),
        ("99999999999", 11) // Max length of Int as String is 10
      )

    stringsAndLengths.foreach { case (str, length) =>
      val bytes = str.getBytes(UTF_8)
      assert(ParserUtils.byteArrayStringToInt(bytes, length) == -1)
    }
  }
}
