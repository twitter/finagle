package com.twitter.finagle.netty4.proxy

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.proxy.{ProxyConnectException, ProxyHandler}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, OneInstancePerTest}
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class Netty4ProxyConnectHandlerTest extends FunSuite with OneInstancePerTest {

  val fakeAddress = InetSocketAddress.createUnresolved("proxy", 0)

  class FakeProxyHandler extends ProxyHandler(fakeAddress) {
    override def removeDecoder(ctx: ChannelHandlerContext): Unit = ()
    override def removeEncoder(ctx: ChannelHandlerContext): Unit = ()
    override def protocol(): String = "proxy"
    override def authScheme(): String = "auth"
    override def addCodec(ctx: ChannelHandlerContext): Unit = ()

    override def handleResponse(ctx: ChannelHandlerContext, response: Any): Boolean = true
    override def newInitialMessage(ctx: ChannelHandlerContext): AnyRef =
      Unpooled.wrappedBuffer("connect".getBytes())
  }

  val (handler, channel) = {
    val hd = new Netty4ProxyConnectHandler(new FakeProxyHandler)
    val ch = new EmbeddedChannel(hd)

    (hd, ch)
  }

  test("success") {
    val promise = channel.connect(fakeAddress)
    assert(!promise.isDone)

    channel.writeOutbound("foo")
    channel.readOutbound[ByteBuf]().release() // drops the proxy handshake message

    assert(channel.readOutbound[Any]() == null)
    channel.writeInbound(Unpooled.wrappedBuffer("connected".getBytes))
    assert(promise.isDone)

    assert(channel.readOutbound[String]() == "foo")

    assert(channel.pipeline().get(classOf[Netty4ProxyConnectHandler]) == null)
    assert(!channel.finishAndReleaseAll())
  }

  test("failure") {
    val promise = channel.connect(fakeAddress)
    assert(!promise.isDone)

    channel.writeOutbound("foo")
    channel.readOutbound[ByteBuf]().release() // drops the proxy handshake message

    assert(channel.readOutbound[String]() == null)

    channel.pipeline().fireExceptionCaught(new Exception())
    assert(promise.isDone)

    assert(intercept[Exception](channel.checkException()).isInstanceOf[ProxyConnectException])

    assert(promise.cause.isInstanceOf[ProxyConnectException])
    assert(!channel.finishAndReleaseAll())
  }
}
