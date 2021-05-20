package com.twitter.finagle.netty4.transport

import com.twitter.finagle.ssl.session.NullSslSessionInfo
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import java.security.cert.{Certificate, X509Certificate}
import javax.net.ssl.{SSLEngine, SSLSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ChannelTransportContextTest extends AnyFunSuite with MockitoSugar {

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

  test("sslSessionInfo is not used by default") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    assert(context.sslSessionInfo == NullSslSessionInfo)
  }

  test("sslSessionInfo is created based on an existing SSLSession") {
    val cert = mock[X509Certificate]
    val certs = Array[Certificate](cert)
    val session = mock[SSLSession]
    when(session.getId).thenReturn("abcd".getBytes)
    when(session.getLocalCertificates).thenReturn(Array[Certificate]())
    when(session.getPeerCertificates).thenReturn(certs)
    val engine = mock[SSLEngine]
    when(engine.getSession).thenReturn(session)
    val sslHandler = new SslHandler(engine)
    val ch = new EmbeddedChannel(sslHandler)
    val context = new ChannelTransportContext(ch)
    assert(context.sslSessionInfo.peerCertificates.nonEmpty)
  }
}
