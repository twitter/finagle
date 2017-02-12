package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ByteArraysTest extends FunSuite {
  test("ByteArrays.concat: add two arrays") {
    val a = Array[Byte](0, 1, 2)
    val b = Array[Byte](3, 4)
    val c = ByteArrays.concat(a, b)
    assert(c === Array[Byte](0, 1, 2, 3, 4))
  }

  test("ByteArrays.concat: empty on the left") {
    val a = Array[Byte]()
    val b = Array[Byte](3, 4)
    val c = ByteArrays.concat(a, b)
    assert(c === Array[Byte](3, 4))
  }

  test("ByteArrays.concat: empty on the right") {
    val a = Array[Byte](0, 1, 2)
    val b = Array[Byte]()
    val c = ByteArrays.concat(a, b)
    assert(c === Array[Byte](0, 1, 2))
  }

  test("ByteArrays.concat: empty on both sides") {
    val a = Array[Byte]()
    val b = Array[Byte]()
    val c = ByteArrays.concat(a, b)
    assert(c.isEmpty)
  }

  test("ByteArrays.put64be, ByteArrays.get64be: write a long and read it back again") {
    val a = new Array[Byte](8)
    ByteArrays.put64be(a, 0, Long.MaxValue)
    assert(ByteArrays.get64be(a, 0) == Long.MaxValue)
  }

  test("ByteArrays.put64be, ByteArrays.get64be: write a long at non-zero offset and read it back again") {
    val a = new Array[Byte](9)
    ByteArrays.put64be(a, 1, Long.MaxValue)
    assert(ByteArrays.get64be(a, 1) == Long.MaxValue)
  }
}
