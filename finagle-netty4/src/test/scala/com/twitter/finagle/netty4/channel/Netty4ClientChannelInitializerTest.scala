package com.twitter.finagle.netty4.channel

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.proxy.Netty4ProxyConnectHandler
import com.twitter.util.{Await, Promise}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.proxy.ProxyConnectException
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class Netty4ClientChannelInitializerTest extends AnyFunSuite {

  test("framed channel initializer releases direct bufs") {
    val e = new EmbeddedChannel()
    val init = new Netty4ClientChannelInitializer(Params.empty, None)
    init.initChannel(e)

    val direct = Unpooled.directBuffer(10)
    direct.writeBytes((1 to 10).toArray.map(_.toByte))

    assert(direct.refCnt() == 1)
    e.writeInbound(direct)
    assert(direct.refCnt() == 0)
  }

  test("raw channel initializer exposes netty pipeline") {
    val reverser = new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit =
        msg match {
          case b: ByteBuf =>
            val bytes = new Array[Byte](b.readableBytes)
            b.readBytes(bytes)
            val reversed = Unpooled.wrappedBuffer(bytes.reverse)
            super.write(ctx, reversed, promise)
          case _ => fail("expected ByteBuf message")
        }
    }

    val init =
      new RawNetty4ClientChannelInitializer(
        pipelineInit = _.addLast(reverser),
        params = Params.empty
      )

    val channel: SocketChannel = new NioSocketChannel()
    val loop = new NioEventLoopGroup()
    loop.register(channel)
    init.initChannel(channel)

    val msgSeen = new Promise[ByteBuf]
    channel.pipeline.addFirst(new ChannelOutboundHandlerAdapter {
      override def write(
        ctx: ChannelHandlerContext,
        msg: scala.Any,
        promise: ChannelPromise
      ): Unit = msg match {
        case b: ByteBuf => msgSeen.setValue(b)
        case _ => fail("expected ByteBuf message")
      }
    })

    val bytes = Array(1.toByte, 2.toByte, 3.toByte)
    channel.writeAndFlush(Unpooled.wrappedBuffer(bytes))

    val seen = new Array[Byte](3)
    Await.result(msgSeen, 5.seconds).readBytes(seen)
    assert(seen.toList == bytes.reverse.toList)
  }

  test("abstract channel initializer bypasses SOCKS5 proxied connections to localhost") {
    val proxyParams = Stack.Params.empty +
      Transporter.SocksProxy(Some(new InetSocketAddress(0)), None)
    val init = new AbstractNetty4ClientChannelInitializer(proxyParams) {}
    val e = new EmbeddedChannel(init)
    e.connect(new InetSocketAddress("localhost", 12345))
    assert(e.pipeline.get(classOf[Netty4ProxyConnectHandler]) == null)
    assert(!e.finish())
  }

  test(
    "abstract channel initializer doesn't bypass SOCKS5 proxied connections to localhost when asked not to") {
    val proxyParams = Stack.Params.empty +
      Transporter.SocksProxy(Some(new InetSocketAddress(0)), None, false)
    val init = new AbstractNetty4ClientChannelInitializer(proxyParams) {}
    val e = new EmbeddedChannel(init)
    e.connect(new InetSocketAddress("localhost", 12345))
    assert(e.pipeline.get(classOf[Netty4ProxyConnectHandler]) != null)
    assertThrows[ProxyConnectException](e.finish())
  }
}
