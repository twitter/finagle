package com.twitter.finagle.thrift

import org.jboss.netty.buffer.ChannelBuffer
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DuplexChannelBufferTransportTest extends FunSuite with MockitoSugar {

  class DuplexChannelContext {
    val in: ChannelBuffer = mock[ChannelBuffer]
    val out: ChannelBuffer = mock[ChannelBuffer]
    val t = new DuplexChannelBufferTransport(in, out)
  }

  val bb = "hello".getBytes

  test("DuplexChannelBufferTransport writes to the output ChannelBuffer") {
    val c = new DuplexChannelContext
    import c._

    t.write(bb, 0, 1)
    verify(out).writeBytes(bb, 0, 1)

    t.write(bb, 1, 2)
    verify(out).writeBytes(bb, 1, 2)

    t.write(bb, 0, 5)
    verify(out).writeBytes(bb, 1, 2)
  }

  test("DuplexChannelBufferTransport reads from the input ChannelBuffer") {
    val c = new DuplexChannelContext
    import c._

    val nReadable = 5
    when(in.readableBytes).thenReturn(nReadable)
    val b = new Array[Byte](nReadable)
    assert(t.read(b, 0, 10) == nReadable)
    assert(t.read(b, 0, 3) == 3)
  }
}
