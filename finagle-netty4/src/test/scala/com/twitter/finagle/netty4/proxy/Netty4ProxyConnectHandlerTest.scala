package com.twitter.finagle.netty4.proxy

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.proxy.{ProxyHandler, Socks5ProxyHandler}
import io.netty.util.concurrent.DefaultPromise
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, OneInstancePerTest}
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class Netty4ProxyConnectHandlerTest extends FunSuite with OneInstancePerTest {

  val fakeAddress = InetSocketAddress.createUnresolved("socks5", 8081)

  val (handler, channel) = {
    // We use `Socks5ProxyHandler` in this test, but it doesn't really matter since all tests
    // here rely on `connectPromise` only.
    val hd = new Netty4ProxyConnectHandler(new Socks5ProxyHandler(fakeAddress))
    val ch = new EmbeddedChannel(hd)

    (hd, ch)
  }

  test("upgrades/downgrades the pipeline") {
    assert(channel.pipeline().get(classOf[ProxyHandler]) != null)
    channel.pipeline().remove(handler)
    assert(channel.pipeline().get(classOf[ProxyHandler]) == null)
  }

  test("success") {
    val connectPromise = handler.connectPromise.asInstanceOf[DefaultPromise[Channel]]
    assert(!connectPromise.isDone)

    val promise = channel.connect(fakeAddress)
    assert(!promise.isDone)

    channel.writeOutbound("foo")
    channel.readOutbound[ByteBuf]().release() // drops the proxy handshake message
    channel.readOutbound[ByteBuf]().release() // drops the proxy handshake message

    assert(channel.readOutbound[Any]() == null)

    connectPromise.setSuccess(channel)
    assert(promise.isDone)

    assert(channel.readOutbound[String]() == "foo")

    assert(channel.pipeline().get(classOf[Netty4ProxyConnectHandler]) == null)
    assert(!channel.finishAndReleaseAll())
  }

  test("failure") {
    val connectPromise = handler.connectPromise.asInstanceOf[DefaultPromise[Channel]]
    assert(!connectPromise.isDone)

    val promise = channel.connect(fakeAddress)
    assert(!promise.isDone)

    channel.writeOutbound("foo")
    channel.readOutbound[ByteBuf]().release() // drops the proxy handshake message
    channel.readOutbound[ByteBuf]().release() // drops the proxy handshake message

    assert(channel.readOutbound[String]() == null)

    val failure = new Exception()
    connectPromise.setFailure(failure)
    assert(promise.isDone)

    assert(intercept[Exception](channel.checkException()) == failure)

    assert(promise.cause == failure)
    assert(!channel.finishAndReleaseAll())
  }
}
