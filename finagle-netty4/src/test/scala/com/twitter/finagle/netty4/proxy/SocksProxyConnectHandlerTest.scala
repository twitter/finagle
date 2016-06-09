package com.twitter.finagle.netty4.proxy

import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.proxy.Socks5ProxyHandler
import io.netty.util.concurrent.DefaultPromise
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{OneInstancePerTest, FunSuite}
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class SocksProxyConnectHandlerTest extends FunSuite with OneInstancePerTest {

  val fakeAddress = InetSocketAddress.createUnresolved("socks5", 8081)

  val (handler, channel) = {
    val hd = new SocksProxyConnectHandler(fakeAddress, None)
    val ch = new EmbeddedChannel(hd)

    (hd, ch)
  }

  test("upgrades/downgrades the pipeline") {
    assert(channel.pipeline().get(classOf[Socks5ProxyHandler]) != null)
    channel.pipeline().remove(handler)
    assert(channel.pipeline().get(classOf[Socks5ProxyHandler]) == null)
  }

  test("success") {
    val connectPromise = handler.connectPromise.asInstanceOf[DefaultPromise[Channel]]
    assert(!connectPromise.isDone)

    val promise = channel.connect(fakeAddress)
    assert(!promise.isDone)

    connectPromise.setSuccess(channel)
    assert(promise.isDone)

    assert(channel.pipeline().get(classOf[SocksProxyConnectHandler]) == null)
    assert(channel.finishAndReleaseAll())
  }

  test("failure") {
    val connectPromise = handler.connectPromise.asInstanceOf[DefaultPromise[Channel]]
    assert(!connectPromise.isDone)

    val promise = channel.connect(fakeAddress)
    assert(!promise.isDone)

    val failure = new Exception()
    connectPromise.setFailure(failure)
    assert(promise.isDone)
    assert(promise.cause == failure)
    assert(channel.finishAndReleaseAll())
  }
}
