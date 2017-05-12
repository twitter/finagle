package com.twitter.finagle.netty4

import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.channel.RecvByteBufAllocatorProxy
import com.twitter.finagle.toggle.flag
import io.netty.channel.{ChannelPipeline, RecvByteBufAllocator}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4ToggleTest extends FunSuite {

  def checkReceiveBuffer(pipeline: ChannelPipeline, isPooled: Boolean): Unit = {
    val alloc = pipeline.channel.config().getRecvByteBufAllocator[RecvByteBufAllocator]
    assert(alloc.isInstanceOf[RecvByteBufAllocatorProxy] == isPooled)
  }

  test("pooling of receive buffers is toggled off by default") {
    Netty4Listener(pipeline =>
      checkReceiveBuffer(pipeline, isPooled = false), Stack.Params.empty)

    Netty4Transporter.raw(pipeline =>
      checkReceiveBuffer(pipeline, isPooled = false), new SocketAddress { }, Stack.Params.empty)
  }

  test("pooling of receive buffers can be toggled on") {
    flag.overrides.let("com.twitter.finagle.netty4.poolReceiveBuffers", 1.0) {
      Netty4Listener(pipeline =>
        checkReceiveBuffer(pipeline, isPooled = true), Stack.Params.empty)

      Netty4Transporter.raw(pipeline =>
        checkReceiveBuffer(pipeline, isPooled = true), new SocketAddress { }, Stack.Params.empty)
    }
  }

  test("pooling is toggled off by default") {
    Netty4Listener(pipeline =>
      assert(!pipeline.channel.alloc.isDirectBufferPooled), Stack.Params.empty)

    Netty4Transporter.raw(pipeline =>
      assert(!pipeline.channel.alloc.isDirectBufferPooled),
      new SocketAddress { },
      Stack.Params.empty
    )
  }

  test("pooling can be toggled on") {
    flag.overrides.let("com.twitter.finagle.netty4.pooling", 1.0) {
      Netty4Listener(pipeline =>
        assert(pipeline.channel.alloc.isDirectBufferPooled), Stack.Params.empty)

      Netty4Transporter.raw(pipeline =>
        assert(pipeline.channel.alloc.isDirectBufferPooled),
        new SocketAddress {},
        Stack.Params.empty
      )
    }
  }
}
