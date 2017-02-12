package com.twitter.finagle.http.netty

import java.nio.charset.Charset
import org.jboss.netty.buffer._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

// Test assumptions of how Netty works
@RunWith(classOf[JUnitRunner])
class NettyTest extends FunSuite {
  test("compose buffers") {
    val bufferA  = ChannelBuffers.wrappedBuffer("A".getBytes)
    assert(bufferA.readableBytes == 1)

    val bufferB  = ChannelBuffers.wrappedBuffer("B".getBytes)
    assert(bufferB.readableBytes == 1)

    val bufferAB = ChannelBuffers.wrappedBuffer(bufferA, bufferB)
    assert(bufferAB.readableBytes == 2)
    assert(bufferAB.toString(Charset.forName("UTF-8")) == "AB")

    val bufferC   = ChannelBuffers.wrappedBuffer("C".getBytes)
    val bufferABC = ChannelBuffers.wrappedBuffer(bufferAB, bufferC)
    assert(bufferABC.readableBytes == 3)
    assert(bufferABC.toString(Charset.forName("UTF-8")) == "ABC")
  }

  test("empty buffers are immutable") {
    assert {
      try {
        ChannelBuffers.EMPTY_BUFFER.writeInt(23)
        false
      } catch {
        case _: IndexOutOfBoundsException => true
        case _: Throwable => false
      }
    }
  }
}
