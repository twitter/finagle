package com.twitter.finagle.memcached.unit.util

import scala.util.Random

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.{Buf, Charsets}

@RunWith(classOf[JUnitRunner])
class ParserUtilsTest extends FunSuite {

  private def isDigitsBB(str: String): Boolean = {
    val bb = Charsets.Utf8.encode(str)
    ParserUtils.isDigits(Buf.ByteBuffer.Owned(bb))
  }

  private def isDigitsBA(str: String): Boolean = {
    val bytes = str.getBytes(Charsets.Utf8)
    ParserUtils.isDigits(Buf.ByteArray.Owned(bytes))
  }

  private def isDigitsBuf(str: String): Boolean = {
    val buf = new TestBuf(str.getBytes(Charsets.Utf8))
    ParserUtils.isDigits(buf)
  }

  // needed to test the non Buf.ByteBuffer/Buf.ByteArray path
  private class TestBuf(bytes: Array[Byte]) extends Buf {
    override def write(output: Array[Byte], off: Int): Unit =
      System.arraycopy(bytes, 0, output, off, length)
    override def length: Int = bytes.length
    override def slice(i: Int, j: Int): Buf = new TestBuf(bytes.drop(i).take(j))
    protected def unsafeByteArrayBuf = None
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

  test("isDigits: Buf") {
    strings foreach { case (str, expected) =>
      assert(isDigitsBuf(str) == expected)
    }
  }

  test("isDigits: Buf.ByteBuffer") {
    strings foreach { case (str, expected) =>
      assert(isDigitsBB(str) == expected)
    }
  }

  test("isDigits: Buf.ByteArray") {
    strings foreach { case (str, expected) =>
      assert(isDigitsBA(str) == expected)
    }
  }
}
