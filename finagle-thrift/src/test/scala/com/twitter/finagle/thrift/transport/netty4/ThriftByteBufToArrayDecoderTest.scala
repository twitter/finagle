package com.twitter.finagle.thrift.transport.netty4

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import java.nio.charset.StandardCharsets
import org.scalatest.funsuite.AnyFunSuite

class ThriftByteBufToArrayDecoderTest extends AnyFunSuite {

  val decoder = ThriftByteBufToArrayDecoder

  def decode(data: ByteBuf): Array[Byte] = {
    val initialRefCount = data.refCnt()
    val channel = new EmbeddedChannel()
    channel.pipeline().addLast(decoder)

    channel.writeInbound(data)

    val bytes = channel.readInbound[Array[Byte]]()
    assert(channel.inboundMessages().size() == 0)

    // Ensure we released the `ByteBuf`
    assert(data.refCnt() == initialRefCount - 1)
    bytes
  }

  def bytes(str: String): Array[Byte] = str.getBytes(StandardCharsets.UTF_8)
  def str(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)

  test("ThriftChannelBufferDecoder convert buffers to an Array[Byte]") {

    val arr = "hello, world!"
    val buf = Unpooled.wrappedBuffer(bytes(arr))

    assert(str(decode(buf)) == arr)
  }

  test("ThriftChannelBufferDecoder convert buffers with an offset to an Array[Byte]") {

    val arr = "hello, world!"
    val prefix = "foo"

    val buf = Unpooled.wrappedBuffer(bytes(prefix ++ arr))
    buf.readBytes(bytes(prefix).length) // discarded

    assert(str(decode(buf)) == arr)
  }

  test("ThriftChannelBufferDecoder converts a composite buffer to an Array[Byte]") {

    val arr = "hello, world!"

    val composite = Unpooled.compositeBuffer()
    composite.addComponent(true, Unpooled.wrappedBuffer(bytes(arr).take(2)))
    composite.addComponent(true, Unpooled.wrappedBuffer(bytes(arr).drop(2)))

    assert(str(decode(composite)) == "hello, world!")
  }
}
