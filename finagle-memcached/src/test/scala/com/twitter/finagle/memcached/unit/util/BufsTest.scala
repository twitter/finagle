package com.twitter.finagle.memcached.unit.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.memcached.util.Bufs.RichBuf
import com.twitter.io.Buf

@RunWith(classOf[JUnitRunner])
class BufsTest extends FunSuite {

  test("RichBuf.toInt") {
    val buf = Buf.Utf8(Int.MaxValue.toString)
    assert(RichBuf(buf).toInt == Int.MaxValue)
  }

  test("RichBuf.toLong") {
    val buf = Buf.Utf8(Long.MaxValue.toString)
    assert(RichBuf(buf).toLong == Long.MaxValue)
  }

  test("RichBuf.apply") {
    val str = "12345"
    val expectedBytes = str.getBytes
    val buf = RichBuf(Buf.Utf8("12345"))
    (1 until str.length) foreach { idx =>
      assert(buf(idx) == expectedBytes(idx))
    }
  }

  test("RichBuf.split on space") {
    val splits = RichBuf(Buf.Utf8("hello world")).split(' ')
    assert(splits(0) == Buf.Utf8("hello"))
    assert(splits(1) == Buf.Utf8("world"))
  }

  test("RichBuf.split on comma") {
    val splits = RichBuf(Buf.Utf8("hello,world")).split(',')
    assert(splits(0) == Buf.Utf8("hello"))
    assert(splits(1) == Buf.Utf8("world"))
  }

  test("RichBuf.startsWith") {
    val buf = RichBuf(Buf.Utf8("hello world"))

    assert(buf.startsWith(Buf.Utf8("")))
    assert(buf.startsWith(Buf.Utf8("h")))
    assert(buf.startsWith(Buf.Utf8("he")))
    assert(buf.startsWith(Buf.Utf8("hel")))
    assert(buf.startsWith(Buf.Utf8("hell")))
    assert(buf.startsWith(Buf.Utf8("hello")))
    assert(buf.startsWith(Buf.Utf8("hello ")))
    assert(buf.startsWith(Buf.Utf8("hello w")))
    assert(buf.startsWith(Buf.Utf8("hello wo")))
    assert(buf.startsWith(Buf.Utf8("hello wor")))
    assert(buf.startsWith(Buf.Utf8("hello worl")))
    assert(buf.startsWith(Buf.Utf8("hello world")))

    assert(false == buf.startsWith(Buf.Utf8("a")))
    assert(false == buf.startsWith(Buf.Utf8(" ")))
  }
}
