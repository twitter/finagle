package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.server.SslServerSessionVerifier
import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import io.netty.util.concurrent.DefaultPromise
import java.security.cert.Certificate
import java.security.cert.X509Certificate
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLSession
import javax.security.auth.x500.X500Principal
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
  val statsReceiver = new InMemoryStatsReceiver
  when(sslHandler.handshakeFuture()).thenReturn(handshakePromise)
  when(sslHandler.engine()).thenReturn(sslEngine)

  test("handler removes itself on successful verification") {
    val pipeline = channel.pipeline
    pipeline.addFirst(
      new SslServerVerificationHandler(
        sslHandler,
        Address.failing,
        sslConfig,
        new TestVerifier(true),
        statsReceiver
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
        new TestVerifier(false),
        statsReceiver
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
        new TestVerifier(throw new Exception("failed verification")),
        statsReceiver
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
        new TestVerifier(false),
        statsReceiver
      )
    )

    handshakePromise.setSuccess(channel)

    assert(!channel.isOpen)

    channel.finishAndReleaseAll()
  }

  test("tracks SSL handshake failure with service identifier metric") {
    val testChannel = new EmbeddedChannel()
    val testStatsReceiver = new InMemoryStatsReceiver
    val testSslHandler = mock[SslHandler]
    val testSslEngine = mock[SSLEngine]
    val testSslSession = mock[SSLSession]
    val testHandshakePromise = new DefaultPromise[Channel](testChannel.eventLoop())

    // Create a mock X509Certificate with the service identifier in CN
    val mockCert = mock[X509Certificate]
    val serviceId = "twtr:svc:admin-service:tss-service:prod:atla"
    val principal = new X500Principal(s"CN=$serviceId")
    when(mockCert.getSubjectX500Principal).thenReturn(principal)
    when(mockCert.getSubjectAlternativeNames).thenReturn(null) // No SAN, extract from CN

    val certs: Array[Certificate] = Array(mockCert)

    when(testSslHandler.handshakeFuture()).thenReturn(testHandshakePromise)
    when(testSslHandler.engine()).thenReturn(testSslEngine)
    when(testSslEngine.getSession).thenReturn(testSslSession)
    when(testSslSession.getPeerCertificates).thenReturn(certs)

    val pipeline = testChannel.pipeline
    pipeline.addFirst(
      new SslServerVerificationHandler(
        testSslHandler,
        Address.failing,
        sslConfig,
        new TestVerifier(true),
        testStatsReceiver
      )
    )

    // Simulate handshake failure
    testHandshakePromise.setFailure(new Exception("PKIX path building failed"))

    // Verify the metric was incremented with the service identifier
    val expectedMetric = testStatsReceiver.counters.keys.find(_.contains(serviceId))
    assert(
      expectedMetric.isDefined,
      s"Expected metric with service ID '$serviceId' not found. Available metrics: ${testStatsReceiver.counters.keys
        .mkString(", ")}"
    )
    assert(
      testStatsReceiver.counters(expectedMetric.get) == 1,
      s"Expected counter to be 1, got ${testStatsReceiver.counters(expectedMetric.get)}")

    assert(!testChannel.isOpen)
    testChannel.finishAndReleaseAll()
  }

  test("tracks SSL handshake failure with no_peer_cert when certificate is missing") {
    val testChannel = new EmbeddedChannel()
    val testStatsReceiver = new InMemoryStatsReceiver
    val testSslHandler = mock[SslHandler]
    val testSslEngine = mock[SSLEngine]
    val testSslSession = mock[SSLSession]
    val testHandshakePromise = new DefaultPromise[Channel](testChannel.eventLoop())

    when(testSslHandler.handshakeFuture()).thenReturn(testHandshakePromise)
    when(testSslHandler.engine()).thenReturn(testSslEngine)
    when(testSslEngine.getSession).thenReturn(testSslSession)
    when(testSslSession.getPeerCertificates).thenReturn(Array.empty[Certificate])

    val pipeline = testChannel.pipeline
    pipeline.addFirst(
      new SslServerVerificationHandler(
        testSslHandler,
        Address.failing,
        sslConfig,
        new TestVerifier(true),
        testStatsReceiver
      )
    )

    // Simulate handshake failure
    testHandshakePromise.setFailure(new Exception("No peer certificate"))

    // Verify the metric was incremented with no_peer_cert
    val expectedMetric = testStatsReceiver.counters.keys.find(_.contains("no_peer_cert"))
    assert(
      expectedMetric.isDefined,
      s"Expected metric with 'no_peer_cert' not found. Available metrics: ${testStatsReceiver.counters.keys
        .mkString(", ")}")
    assert(testStatsReceiver.counters(expectedMetric.get) == 1)

    assert(!testChannel.isOpen)
    testChannel.finishAndReleaseAll()
  }
}
