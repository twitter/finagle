package com.twitter.finagle.thrift

import org.jboss.netty.buffer.ChannelBuffer
import org.junit.runner.RunWith
import org.mockito.Mockito.{verify, when}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ChannelBufferTransportTest extends FunSuite with MockitoSugar {

  case class ChannelContext(buf: ChannelBuffer){
    lazy val t = new ChannelBufferToTransport(buf)
  }
  val bb = "hello".getBytes

  test("ChannelBufferToTransport writes bytes to the underlying ChannelBuffer") {
    val c = ChannelContext(mock[ChannelBuffer])
    import c._

    t.write(bb, 0, 1)
    verify(buf).writeBytes(bb, 0, 1)

    t.write(bb, 1, 2)
    verify(buf).writeBytes(bb, 1, 2)

    t.write(bb, 0, 5)
    verify(buf).writeBytes(bb, 1, 2)
  }

  test("ChannelBufferToTransport reads bytes from the underlying ChannelBuffer") {
    val c = ChannelContext(mock[ChannelBuffer])
    import c._

    val nReadable = 5
    when(buf.readableBytes).thenReturn(nReadable)
    val b = new Array[Byte](nReadable)
    assert(t.read(b, 0, 10) === nReadable)
    assert(t.read(b, 0, 3) === 3)
  }
}

@RunWith(classOf[JUnitRunner])
class DuplexChannelBufferTransportTest extends FunSuite with MockitoSugar {

  case class DuplexChannelContext(in: ChannelBuffer, out: ChannelBuffer) {
    lazy val t = new DuplexChannelBufferTransport(in, out)
  }

  val bb = "hello".getBytes

  test("DuplexChannelBufferTransport writes to the output ChannelBuffer") {
    val c = DuplexChannelContext(mock[ChannelBuffer], mock[ChannelBuffer])
    import c._

    t.write(bb, 0, 1)
    verify(out).writeBytes(bb, 0, 1)

    t.write(bb, 1, 2)
    verify(out).writeBytes(bb, 1, 2)

    t.write(bb, 0, 5)
    verify(out).writeBytes(bb, 1, 2)
  }

  test("DuplexChannelBufferTransport reads from the input ChannelBuffer") {
    val c = DuplexChannelContext(mock[ChannelBuffer], mock[ChannelBuffer])
    import c._

    val nReadable = 5
    when(in.readableBytes).thenReturn(nReadable)
    val b = new Array[Byte](nReadable)
    assert(t.read(b, 0, 10) === nReadable)
    assert(t.read(b, 0, 3) === 3)
  }
}
