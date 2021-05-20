package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.{Address, SslVerificationFailedException}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.DefaultPromise
import java.net.InetSocketAddress
import javax.net.ssl.{SSLEngine, SSLSession}
import org.mockito.Mockito._
import org.scalatest.OneInstancePerTest
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class SslClientVerificationHandlerTest
    extends AnyFunSuite
    with MockitoSugar
    with OneInstancePerTest {

  val fakeAddress = InetSocketAddress.createUnresolved("ssl", 8081)
  val address = Address.Inet(fakeAddress, Map.empty)
  val config = SslClientConfiguration()

  class TestVerifier(result: => Boolean) extends SslClientSessionVerifier {
    def apply(address: Address, config: SslClientConfiguration, session: SSLSession): Boolean =
      result
  }

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
    channel
      .pipeline()
      .addFirst(
        new SslClientVerificationHandler(
          sslHandler,
          address,
          config,
          SslClientSessionVerifier.AlwaysValid
        )
      )
    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    handshakePromise.setSuccess(channel)
    assert(connectPromise.isSuccess)
    assert(channel.readOutbound[String]() == "pending write")

    channel.finishAndReleaseAll()
  }

  test("session verification failed") {
    channel
      .pipeline()
      .addFirst(
        new SslClientVerificationHandler(
          sslHandler,
          address,
          config,
          new TestVerifier(false)
        )
      )
    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    handshakePromise.setSuccess(channel)
    assert(connectPromise.cause().isInstanceOf[SslVerificationFailedException])
    assert(
      intercept[Exception](channel.checkException()).isInstanceOf[SslVerificationFailedException]
    )

    channel.finishAndReleaseAll()
  }

  test("failed session validation") {
    val e = new Exception("whoa")
    channel
      .pipeline()
      .addFirst(
        new SslClientVerificationHandler(
          sslHandler,
          address,
          config,
          new TestVerifier(throw e)
        )
      )
    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    handshakePromise.setSuccess(channel)
    assert(connectPromise.cause.getMessage.startsWith("whoa"))
    assert(
      intercept[Exception](channel.checkException()).isInstanceOf[SslVerificationFailedException]
    )

    channel.finishAndReleaseAll()
  }

  test("cancelled after connected") {
    channel
      .pipeline()
      .addFirst(
        new SslClientVerificationHandler(
          sslHandler,
          address,
          config,
          SslClientSessionVerifier.AlwaysValid
        )
      )
    val connectPromise = channel.connect(fakeAddress)
    assert(!connectPromise.isDone)

    assert(connectPromise.cancel(true))
    assert(!channel.isActive)

    channel.finishAndReleaseAll()
  }

  test("failed handshake") {
    channel
      .pipeline()
      .addFirst(
        new SslClientVerificationHandler(
          sslHandler,
          address,
          config,
          SslClientSessionVerifier.AlwaysValid
        )
      )
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

  test("session verification failed without connect") {
    channel
      .pipeline()
      .addFirst(
        new SslClientVerificationHandler(
          sslHandler,
          address,
          config,
          new TestVerifier(false)
        )
      )

    channel.writeOutbound("pending write")
    assert(channel.outboundMessages().size() == 0)

    assert(channel.isOpen)
    handshakePromise.setSuccess(channel)

    assert(
      intercept[Exception](channel.checkException()).isInstanceOf[SslVerificationFailedException]
    )

    channel.finishAndReleaseAll()
  }
}
