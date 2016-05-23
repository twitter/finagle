package com.twitter.finagle.netty4.channel

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.Params
import com.twitter.util.{Await, Promise}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4ClientChannelInitializerTest extends FunSuite {

  test("raw channel initializer exposes netty pipeline") {
    val reverser = new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = msg match {
        case b: ByteBuf =>
          val bytes = new Array[Byte](b.readableBytes)
          b.readBytes(bytes)
          val reversed = Unpooled.wrappedBuffer(bytes.reverse)
          super.write(ctx, reversed, promise)
        case _ => fail("expected ByteBuf message")
      }
    }

    val init =
      new RawNetty4ClientChannelInitializer[ByteBuf, ByteBuf](
        pipelineInit = _.addLast(reverser),
        params = Params.empty)

    val channel: SocketChannel = new NioSocketChannel()
    val loop = new NioEventLoopGroup()
    loop.register(channel)
    init.initChannel(channel)

    val msgSeen = new Promise[ByteBuf]
    channel.pipeline.addFirst(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = msg match {
        case b: ByteBuf => msgSeen.setValue(b)
        case _ => fail("expected ByteBuf message")
      }
    })

    val bytes = Array(1.toByte, 2.toByte, 3.toByte)
    channel.write(Unpooled.wrappedBuffer(bytes))

    val seen = new Array[Byte](3)
    Await.result(msgSeen, 5.seconds).readBytes(seen)
    assert(seen.toList == bytes.reverse.toList)
  }
}
