package com.twitter.finagle.memcached.unit.util

import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf
import java.nio.charset.StandardCharsets.UTF_8
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class ParserUtilsTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

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
      "1" -> true,
      "" -> false,
      " " -> false,
      "x" -> false,
      " 9" -> false,
      "9 " -> false
    )

  test("isDigits: Buf.ByteBuffer") {
    strings.foreach {
      case (str, expected) =>
        withClue(str) {
          assert(isDigitsBB(str) == expected)
        }
    }
  }

  test("isDigits: Buf.ByteArray") {
    strings.foreach {
      case (str, expected) =>
        withClue(str) {
          assert(isDigitsBA(str) == expected)
        }
    }
  }

  test("ByteArrayString to positive Int") {
    forAll { num: Int =>
      val buf = Buf.ByteArray.Owned(num.toString.getBytes(UTF_8))
      val asInt = ParserUtils.bufToInt(buf)
      if (num >= 0)
        assert(asInt == num)
      else
        assert(asInt == -1)
    }

    // check cases where the byte array is an invalid Int String or length is invalid
    Seq(
      "xxxx",
      "999999999999" // Max length of Int as String is 10
    ).foreach { str =>
      val buf = Buf.ByteArray.Owned(str.getBytes(UTF_8))
      assert(ParserUtils.bufToInt(buf) == -1)
    }
  }
}
