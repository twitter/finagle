package com.twitter.finagle.netty3

import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.jboss.netty.buffer.ChannelBuffers

@RunWith(classOf[JUnitRunner])
class BufTest extends FunSuite {
  test("ChannelBufferBuf.slice: slices according to the underlying ChannelBuffer") {
    val cb = ChannelBuffers.buffer(128)
    cb.writeBytes(Array[Byte](1,2,3,4,5,6,7,8,9))
    val buf = ChannelBufferBuf.Owned(cb)
    assert(buf.length == 9)
    assert(buf.slice(2,4) == Buf.ByteArray(3,4))
    assert(buf == Buf.ByteArray(1,2,3,4,5,6,7,8,9))
  }

  test("ChannelBufferBuf.slice: truncates on out-of-bounds indices") {
    val cb = ChannelBuffers.buffer(128)
    cb.writeBytes(Array[Byte](1,2,3,4,5,6,7,8,9))
    val buf = ChannelBufferBuf.Owned(cb)
    buf.slice(1, Int.MaxValue)
  }

  test("ChannelBufferBuf.Shared.apply") {
    val bytes = Array[Byte](1,2,3,4,5,6,7,8,9)
    val cb = ChannelBuffers.wrappedBuffer(bytes)
    val buf = ChannelBufferBuf.Shared(cb)
    bytes(0) = 0.toByte
    assert(buf == Buf.ByteArray(1,2,3,4,5,6,7,8,9))
    cb.clear()
    assert(buf == Buf.ByteArray(1,2,3,4,5,6,7,8,9))
  }

  test("ChannelBufferBuf.Direct.apply") {
    val bytes = Array[Byte](1,2,3,4,5,6,7,8,9)
    val cb = ChannelBuffers.wrappedBuffer(bytes)
    val buf = ChannelBufferBuf.Owned(cb)
    bytes(0) = 0.toByte
    assert(buf == Buf.ByteArray(0,2,3,4,5,6,7,8,9))
    cb.clear()
    assert(buf.length == 0)
  }

  test("ChannelBufferBuf.Shared.unapply") {
    val cb0 = ChannelBuffers.buffer(128)
    cb0.writeBytes(Array[Byte](1,2,3,4,5,6,7,8,9))
    val ChannelBufferBuf.Shared(cb1) = ChannelBufferBuf.Owned(cb0)
    cb0.clear()
    assert(cb1.readableBytes() == 9)
  }

  test("ChannelBufferBuf.Direct.unapply") {
    val cb0 = ChannelBuffers.buffer(128)
    cb0.writeBytes(Array[Byte](1,2,3,4,5,6,7,8,9))
    val ChannelBufferBuf.Owned(cb1) = ChannelBufferBuf.Owned(cb0)
    cb0.clear()
    assert(cb1.readableBytes() == 0)
  }

  test("ChannelBufferBuf.coerce(ChannelBufferBuf)") {
    val cb = ChannelBuffers.wrappedBuffer(Array[Byte](1,2,3,4,5,6,7,8,9))
    val orig = ChannelBufferBuf.Owned(cb)
    val coerced = ChannelBufferBuf.coerce(orig)
    assert(coerced eq orig)
  }

  test("ChannelBufferBuf.coerce(ByteArray)") {
    val orig = Buf.ByteArray(1,2,3,4,5,6,7,8,9)
    val coerced = ChannelBufferBuf.coerce(orig)
    assert(coerced == orig)
  }

  test("ByteArray.coerce(ChannelBufferBuf)") {
    val orig = ChannelBufferBuf.Owned(
      ChannelBuffers.wrappedBuffer(Array[Byte](1,2,3,4,5,6,7,8,9), 2, 4))
    val coerced = Buf.ByteArray.coerce(orig)
    assert(coerced == orig)
    val Buf.ByteArray.Owned(bytes, begin, end) = coerced
    assert(bytes.length == 9)
    assert(begin == 2)
    assert(end == 6)
  }
}
