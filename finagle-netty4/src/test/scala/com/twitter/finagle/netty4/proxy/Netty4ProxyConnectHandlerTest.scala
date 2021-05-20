package com.twitter.finagle.netty4.proxy

import com.twitter.finagle.ProxyConnectException
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.proxy.{ProxyHandler, ProxyConnectException => NettyProxyConnectException}
import io.netty.util.concurrent.{Future, GenericFutureListener}
import org.scalatest.OneInstancePerTest
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class Netty4ProxyConnectHandlerTest extends AnyFunSuite with OneInstancePerTest {

  val fakeAddress = InetSocketAddress.createUnresolved("proxy", 0)

  class FakeProxyHandler extends ProxyHandler(fakeAddress) {

    private[this] var removedDecoder = false
    private[this] var removedEncder = false

    def removedCodec: Boolean = removedDecoder && removedEncder

    override def removeDecoder(ctx: ChannelHandlerContext): Unit = {
      removedDecoder = true
    }

    override def removeEncoder(ctx: ChannelHandlerContext): Unit = {
      removedEncder = true
    }

    override def protocol(): String = "proxy"
    override def authScheme(): String = "auth"
    override def addCodec(ctx: ChannelHandlerContext): Unit = ()

    override def handleResponse(ctx: ChannelHandlerContext, response: Any): Boolean = true
    override def newInitialMessage(ctx: ChannelHandlerContext): AnyRef =
      Unpooled.wrappedBuffer("connect".getBytes())
  }

  val (handler, fakeHandler, channel) = {
    val fh = new FakeProxyHandler
    val hd = new Netty4ProxyConnectHandler(fh)
    val ch = new EmbeddedChannel(hd)

    (hd, fh, ch)
  }

  test("canceled before completed connection") {
    val connectPromise = channel.connect(fakeAddress)

    channel.writeOutbound("foo")
    channel.readOutbound[ByteBuf]().release() // drops the proxy handshake message

    assert(!connectPromise.isDone)

    assert(connectPromise.cancel(true))
    assert(!channel.isActive)

    intercept[NettyProxyConnectException] { channel.finishAndReleaseAll() }
  }

  test("success") {
    val promise = channel.connect(fakeAddress)
    assert(!promise.isDone)

    channel.writeOutbound("foo")
    channel.readOutbound[ByteBuf]().release() // drops the proxy handshake message
    assert(channel.readOutbound[Any]() == null)

    promise.addListener(new GenericFutureListener[Future[Any]] {
      def operationComplete(future: Future[Any]): Unit =
        // The codec should be already removed when connect promise is satsfied.
        // See https://github.com/netty/netty/issues/6671
        assert(fakeHandler.removedCodec)
    })

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
    intercept[Exception](channel.checkException())

    assert(promise.isDone)
    assert(promise.cause.isInstanceOf[ProxyConnectException])
    assert(!channel.finishAndReleaseAll())
  }
}
