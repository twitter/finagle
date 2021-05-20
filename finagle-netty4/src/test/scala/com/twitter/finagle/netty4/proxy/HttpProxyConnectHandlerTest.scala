package com.twitter.finagle.netty4.proxy

import com.twitter.finagle.{ChannelClosedException, ProxyConnectException}
import io.netty.buffer.Unpooled
import io.netty.channel.{
  ChannelHandlerAdapter,
  ChannelHandlerContext,
  ChannelOutboundHandlerAdapter,
  ChannelPromise
}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import org.scalatest.OneInstancePerTest
import java.net.{InetSocketAddress, SocketAddress}
import org.scalatest.funsuite.AnyFunSuite

class HttpProxyConnectHandlerTest extends AnyFunSuite with OneInstancePerTest {

  class ConnectPromiseSnooper extends ChannelOutboundHandlerAdapter {
    var promise: ChannelPromise = _
    override def connect(
      ctx: ChannelHandlerContext,
      r: SocketAddress,
      l: SocketAddress,
      p: ChannelPromise
    ): Unit = {
      promise = p
      // drop connect request
    }
  }

  val fakeAddress = InetSocketAddress.createUnresolved("http-proxy", 8081)
  val emptyCodec = new ChannelHandlerAdapter {}
  val connectPromiseSnooper = new ConnectPromiseSnooper

  test("connect to proxy (success)") {
    val channel = new EmbeddedChannel(new HttpProxyConnectHandler("example.com", None, emptyCodec))

    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    val outboundReq = channel.readOutbound[FullHttpRequest]()

    assert(outboundReq.method() == HttpMethod.CONNECT)
    assert(outboundReq.uri() == "example.com")
    assert(outboundReq.headers().get(HttpHeaderNames.HOST) == "example.com")

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK))
    channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT)

    assert(connectPromise.isSuccess)

    assert(channel.readOutbound[String]() == "pending write")
    channel.finishAndReleaseAll()
  }

  test("connect to proxy (canceled before connected)") {
    val channel = new EmbeddedChannel(
      connectPromiseSnooper,
      new HttpProxyConnectHandler("example.com", None, emptyCodec)
    )

    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)
    assert(connectPromise.cancel(false))
    assert(connectPromiseSnooper.promise.isCancelled)

    channel.finishAndReleaseAll()
  }

  test("connect to proxy (canceled after connected)") {
    val channel = new EmbeddedChannel(
      new HttpProxyConnectHandler("example.com", None, emptyCodec)
    )

    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)
    assert(channel.isActive)
    assert(connectPromise.cancel(false))
    assert(!channel.isActive)

    channel.finishAndReleaseAll()
  }

  test("connect to proxy (tcp connect failed)") {
    val e = new Exception("not good")
    val channel = new EmbeddedChannel(
      connectPromiseSnooper,
      new HttpProxyConnectHandler("example.com", None, emptyCodec)
    )

    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    connectPromiseSnooper.promise.setFailure(e)
    assert(connectPromise.cause() == e)

    channel.finishAndReleaseAll()
  }

  test("connect to proxy (http connect failed)") {
    val channel = new EmbeddedChannel(new HttpProxyConnectHandler("example.com", None, emptyCodec))

    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)
    assert(channel.readOutbound[FullHttpRequest]() != null)

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    val rep = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.BAD_REQUEST,
      Unpooled.wrappedBuffer("do not talk to me ever again".getBytes("UTF-8"))
    )

    assert(
      intercept[Exception](channel.writeInbound(rep.retain())).isInstanceOf[ProxyConnectException]
    )
    assert(!connectPromise.isSuccess)
    assert(connectPromise.cause().isInstanceOf[ProxyConnectException])
    assert(rep.release())

    channel.finishAndReleaseAll()
  }

  test("connect to proxy (exception caught after connected)") {
    val channel = new EmbeddedChannel(new HttpProxyConnectHandler("example.com", None, emptyCodec))

    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)
    assert(channel.readOutbound[FullHttpRequest]() != null)

    channel.writeOutbound("pending write")

    val e = new Exception("not good")
    channel.pipeline().fireExceptionCaught(e)

    assert(!connectPromise.isSuccess)
    assert(connectPromise.cause() == e)
    assert(intercept[Exception](channel.checkException()) == e)

    channel.finishAndReleaseAll()
  }

  test("connect to proxy (channel closed after connected)") {
    val channel = new EmbeddedChannel(
      new HttpProxyConnectHandler("example.com", None, emptyCodec)
    )

    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)
    assert(channel.isActive)
    channel.close().sync()
    assert(!channel.isActive)
    assert(connectPromise.cause().isInstanceOf[ChannelClosedException])

    channel.finishAndReleaseAll()
  }
}
