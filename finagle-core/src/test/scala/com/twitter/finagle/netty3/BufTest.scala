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
    val buf = ChannelBufferBuf(cb)
    assert(buf.length === 9)
    assert(buf.slice(2,4) === Buf.ByteArray(3,4))
    assert(buf === Buf.ByteArray(1,2,3,4,5,6,7,8,9))
  }

  test("ChannelBufferBuf.slice: truncates on out-of-bounds indices") {
    val cb = ChannelBuffers.buffer(128)
    cb.writeBytes(Array[Byte](1,2,3,4,5,6,7,8,9))
    val buf = ChannelBufferBuf(cb)
    buf.slice(1, Int.MaxValue)
  }
}
