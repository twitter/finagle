package com.twitter.finagle.http.netty

import java.nio.charset.Charset
import org.jboss.netty.buffer._
import org.specs.SpecificationWithJUnit


// Test assumptions of how Netty works
class NettySpec extends SpecificationWithJUnit {
  "netty" should {
    "compose buffers" in {
      val bufferA  = ChannelBuffers.wrappedBuffer("A".getBytes)
      bufferA.readableBytes must_== 1
      val bufferB  = ChannelBuffers.wrappedBuffer("B".getBytes)
      bufferB.readableBytes must_== 1
      val bufferAB = ChannelBuffers.wrappedBuffer(bufferA, bufferB)
      bufferAB.readableBytes                      must_== 2
      bufferAB.toString(Charset.forName("UTF-8")) must_== "AB"

      val bufferC   = ChannelBuffers.wrappedBuffer("C".getBytes)
      val bufferABC = ChannelBuffers.wrappedBuffer(bufferAB, bufferC)
      bufferABC.readableBytes  must_== 3
      bufferABC.toString(Charset.forName("UTF-8")) must_== "ABC"
    }

    "empty buffers are immutable" in {
      ChannelBuffers.EMPTY_BUFFER.writeInt(23) must throwA[IndexOutOfBoundsException]
    }
 }
}
