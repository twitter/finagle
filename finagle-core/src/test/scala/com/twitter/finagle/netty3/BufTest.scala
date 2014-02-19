package com.twitter.finagle.netty3

import com.twitter.util.{Await, Closable, Future, Time}
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

@RunWith(classOf[JUnitRunner])
class BufTest extends FunSuite {
  test("ChannelBufferBuf") {
    val cb = ChannelBuffers.buffer(128)
    cb.writeBytes(Array[Byte](1,2,3,4,5,6,7,8,9))
    val buf = ChannelBufferBuf(cb)
    assert(buf.length === 9)
    assert(buf.slice(2, 4) === Buf.ByteArray(3,4))
    assert(buf === Buf.ByteArray(1,2,3,4,5,6,7,8,9))
  }
}
