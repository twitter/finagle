package com.twitter.finagle.http2.transport.client
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.io.Buf
import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.funsuite.AnyFunSuite

class DelayByteBufHandlerTest extends AnyFunSuite {

  test("DelayByteHandler emits unread bytes to the pipeline when removed") {
    val buffer = ByteBufConversion.bufAsByteBuf(Buf.Utf8("hi"))
    val channel: EmbeddedChannel = new EmbeddedChannel(new DelayByteBufHandler)
    channel.writeInbound(buffer)
    assert(channel.inboundMessages.size == 0)

    channel.pipeline.remove(classOf[DelayByteBufHandler])
    assert(channel.inboundMessages.size == 1)
  }
}
