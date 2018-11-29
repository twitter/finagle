package com.twitter.finagle.netty4.transport

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import java.security.cert.{Certificate, X509Certificate}
import javax.net.ssl.{SSLEngine, SSLSession}
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.FunSuite

class ChannelTransportContextTest extends FunSuite with MockitoSugar {

  test("localAddress returns channel's local address") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    assert(context.localAddress == ch.localAddress)
  }

  test("remoteAddress returns channel's remote address") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    assert(context.remoteAddress == ch.remoteAddress)
  }

  test("peer certificate is empty by default") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    assert(context.peerCertificate.isEmpty)
  }

  test("peer certificate is retrieved from an existing SSLEngine") {
    val cert = mock[X509Certificate]
    val certs = Array[Certificate](cert)
    val session = mock[SSLSession]
    when(session.getPeerCertificates).thenReturn(certs)
    val engine = mock[SSLEngine]
    when(engine.getSession).thenReturn(session)
    val sslHandler = new SslHandler(engine)
    val ch = new EmbeddedChannel(sslHandler)
    val context = new ChannelTransportContext(ch)
    assert(context.peerCertificate.nonEmpty)
  }

  test("executor is the channel's event loop") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    assert(context.executor == ch.eventLoop)
  }

}
