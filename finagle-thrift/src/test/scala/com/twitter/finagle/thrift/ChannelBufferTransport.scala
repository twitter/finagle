package com.twitter.finagle.thrift

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import org.jboss.netty.buffer.ChannelBuffer

class ChannelBufferTransportSpec extends SpecificationWithJUnit with Mockito {
  "ChannelBufferToTransport" should {
    val buf = mock[ChannelBuffer]
    val t = new ChannelBufferToTransport(buf)
    val bb = "hello".getBytes

    "writing bytes to the underlying ChannelBuffer" in {
      t.write(bb, 0, 1)
      there was one(buf).writeBytes(bb, 0, 1)

      t.write(bb, 1, 2)
      there was one(buf).writeBytes(bb, 1, 2)

      t.write(bb, 0, 5)
      there was one(buf).writeBytes(bb, 1, 2)
    }

    "reading bytes from the underlying ChannelBuffer" in {
      val nReadable = 5
      buf.readableBytes returns nReadable
      val b = new Array[Byte](nReadable)
      t.read(b, 0, 10) mustEqual nReadable
      t.read(b, 0, 3) mustEqual 3
    }
  }
}

class DuplexChannelBufferTransportSpec extends SpecificationWithJUnit with Mockito {
  "DuplexChannelBufferTransport" should {
    val in = mock[ChannelBuffer]
    val out = mock[ChannelBuffer]
    val t = new DuplexChannelBufferTransport(in, out)
    val bb = "hello".getBytes

    "writes to the output ChannelBuffer" in {
      t.write(bb, 0, 1)
      there was one(out).writeBytes(bb, 0, 1)

      t.write(bb, 1, 2)
      there was one(out).writeBytes(bb, 1, 2)

      t.write(bb, 0, 5)
      there was one(out).writeBytes(bb, 1, 2)
    }

    "reading from the input ChannelBuffer" in {
      val nReadable = 5
      in.readableBytes returns nReadable
      val b = new Array[Byte](nReadable)
      t.read(b, 0, 10) mustEqual nReadable
      t.read(b, 0, 3) mustEqual 3
    }
  }
}
