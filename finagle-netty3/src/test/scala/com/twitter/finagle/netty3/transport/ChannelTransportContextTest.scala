package com.twitter.finagle.netty3.transport

import com.twitter.finagle.ssl.session.NullSslSessionInfo
import java.net.InetSocketAddress
import java.security.cert.{Certificate, X509Certificate}
import javax.net.ssl.{SSLEngine, SSLSession}
import org.jboss.netty.channel.{Channel, ChannelPipeline}
import org.jboss.netty.handler.ssl.SslHandler
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class ChannelTransportContextTest extends FunSuite with MockitoSugar {

  test("localAddress returns channel's local address") {
    val ch = mock[Channel]
    when(ch.getLocalAddress).thenReturn(new InetSocketAddress("localhost", 1234))
    val context = new ChannelTransportContext(ch)
    val local = context.localAddress.asInstanceOf[InetSocketAddress]
    assert(local.getHostString == "localhost")
    assert(local.getPort == 1234)
  }

  test("remoteAddress returns channel's remote address") {
    val ch = mock[Channel]
    when(ch.getRemoteAddress).thenReturn(new InetSocketAddress("localhost", 2345))
    val context = new ChannelTransportContext(ch)
    val remote = context.remoteAddress.asInstanceOf[InetSocketAddress]
    assert(remote.getHostString == "localhost")
    assert(remote.getPort == 2345)
  }

  test("sslSessionInfo is not used by default") {
    val ch = mock[Channel]
    val pipeline = mock[ChannelPipeline]
    when(ch.getPipeline).thenReturn(pipeline)
    when(pipeline.get(classOf[SslHandler])).thenReturn(null)
    val context = new ChannelTransportContext(ch)
    assert(context.sslSessionInfo == NullSslSessionInfo)
  }

  test("sslSessionInfo is created from an existing SSLEngine") {
    val cert = mock[X509Certificate]
    val certs = Array[Certificate](cert)
    val session = mock[SSLSession]
    when(session.getId).thenReturn("abcd".getBytes)
    when(session.getLocalCertificates).thenReturn(Array[Certificate]())
    when(session.getPeerCertificates).thenReturn(certs)
    val engine = mock[SSLEngine]
    when(engine.getSession).thenReturn(session)
    val sslHandler = mock[SslHandler]
    when(sslHandler.getEngine).thenReturn(engine)

    val ch = mock[Channel]
    val pipeline = mock[ChannelPipeline]
    when(ch.getPipeline).thenReturn(pipeline)
    when(pipeline.get(classOf[SslHandler])).thenReturn(sslHandler)
    val context = new ChannelTransportContext(ch)
    assert(context.sslSessionInfo.peerCertificates.nonEmpty)
  }

}
