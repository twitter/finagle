package com.twitter.finagle.memcachedx.unit.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.memcachedx.util.Bufs.RichBuf
import com.twitter.io.Buf

@RunWith(classOf[JUnitRunner])
class BufsTest extends FunSuite {

  test("RichBuf.toInt") {
    val buf = Buf.Utf8(Int.MaxValue.toString)
    assert(RichBuf(buf).toInt === Int.MaxValue)
  }

  test("RichBuf.toLong") {
    val buf = Buf.Utf8(Long.MaxValue.toString)
    assert(RichBuf(buf).toLong === Long.MaxValue)
  }

  test("RichBuf.apply") {
    val str = "12345"
    val expectedBytes = str.getBytes
    val buf = RichBuf(Buf.Utf8("12345"))
    (1 until str.length) foreach { idx =>
      assert(buf(idx) === expectedBytes(idx))
    }
  }
}
