package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.DefaultPromise
import javax.net.ssl.{SSLEngine, SSLSession}
import org.mockito.Mockito.when
import org.scalatest.OneInstancePerTest
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class SslServerVerificationHandlerTest
    extends AnyFunSuite
    with MockitoSugar
    with OneInstancePerTest {

  class TestVerifier(result: => Boolean) extends SslServerSessionVerifier {
    def apply(address: Address, config: SslServerConfiguration, session: SSLSession): Boolean =
      result
  }

  val channel = new EmbeddedChannel()
  val sslConfig = mock[SslServerConfiguration]
  val sslHandler = mock[SslHandler]
  val sslEngine = mock[SSLEngine]
  val sslSession = mock[SSLSession]
  val handshakePromise = new DefaultPromise[Channel](channel.eventLoop())
  when(sslHandler.handshakeFuture()).thenReturn(handshakePromise)
  when(sslHandler.engine()).thenReturn(sslEngine)

  test("handler removes itself on successful verification") {
    val pipeline = channel.pipeline
    pipeline.addFirst(
      new SslServerVerificationHandler(
        sslHandler,
        Address.failing,
        sslConfig,
        new TestVerifier(true)
      )
    )

    val before = pipeline.get(classOf[SslServerVerificationHandler])
    assert(before != null)

    pipeline.fireChannelActive()
    handshakePromise.setSuccess(channel)

    val after = pipeline.get(classOf[SslServerVerificationHandler])
    assert(after == null)

    assert(channel.isOpen)

    channel.finishAndReleaseAll()
  }

  test("closes channel when verification fails") {
    val pipeline = channel.pipeline
    pipeline.addFirst(
      new SslServerVerificationHandler(
        sslHandler,
        Address.failing,
        sslConfig,
        new TestVerifier(false)
      )
    )

    pipeline.fireChannelActive()
    handshakePromise.setSuccess(channel)

    assert(!channel.isOpen)

    channel.finishAndReleaseAll()
  }

  test("closes channel when verification throws") {
    val pipeline = channel.pipeline
    pipeline.addFirst(
      new SslServerVerificationHandler(
        sslHandler,
        Address.failing,
        sslConfig,
        new TestVerifier(throw new Exception("failed verification"))
      )
    )

    pipeline.fireChannelActive()
    handshakePromise.setSuccess(channel)

    assert(!channel.isOpen)

    channel.finishAndReleaseAll()
  }

  test("closes channel when verification fails without channel active") {
    val pipeline = channel.pipeline
    pipeline.addFirst(
      new SslServerVerificationHandler(
        sslHandler,
        Address.failing,
        sslConfig,
        new TestVerifier(false)
      )
    )

    handshakePromise.setSuccess(channel)

    assert(!channel.isOpen)

    channel.finishAndReleaseAll()
  }
}
