package com.twitter.finagle.netty4.ssl

import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.DefaultPromise
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, OneInstancePerTest}
import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito._
import java.net.InetSocketAddress
import javax.net.ssl.{SSLEngine, SSLSession}
import org.scalatest.mockito.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SslConnectHandlerTest extends FunSuite with MockitoSugar with OneInstancePerTest {

  val fakeAddress = InetSocketAddress.createUnresolved("ssl", 8081)

  val (channel, sslHandler, handshakePromise) = {
    val ch = new EmbeddedChannel()
    val hd = mock[SslHandler]
    val e = mock[SSLEngine]
    val hp = new DefaultPromise[Channel](ch.eventLoop())
    when(hd.handshakeFuture()).thenReturn(hp)
    when(hd.engine).thenReturn(e)
    when(e.getSession).thenReturn(mock[SSLSession])

    (ch, hd, hp)
  }

  test("success") {
    channel.pipeline().addFirst(new SslConnectHandler(sslHandler, _ => None))
    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    handshakePromise.setSuccess(channel)
    assert(connectPromise.isSuccess)
    assert(channel.readOutbound[String]() == "pending write")

    channel.finishAndReleaseAll()
  }

  test("failed session validation") {
    val e = new Exception("whoa")
    channel.pipeline().addFirst(new SslConnectHandler(sslHandler, _ => Some(e)))
    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    handshakePromise.setSuccess(channel)
    assert(connectPromise.cause() == e)
    assert(intercept[Exception](channel.checkException()) == e)

    channel.finishAndReleaseAll()
  }

  test("cancelled after connected") {
    channel.pipeline().addFirst(new SslConnectHandler(sslHandler, _ => None))
    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    assert(connectPromise.cancel(true))
    assert(!channel.isActive)

    channel.finishAndReleaseAll()
  }

  test("failed handshake") {
    channel.pipeline().addFirst(new SslConnectHandler(sslHandler, _ => None))
    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    val e = new Exception("not so good")
    handshakePromise.setFailure(e)
    assert(!connectPromise.isSuccess)
    assert(intercept[Exception](channel.checkException()) == e)

    channel.finishAndReleaseAll()
  }
}
