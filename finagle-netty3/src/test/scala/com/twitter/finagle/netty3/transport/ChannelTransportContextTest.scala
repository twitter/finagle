package com.twitter.finagle.netty3.transport

import com.twitter.finagle.Status
import com.twitter.util.{Return, Time}
import com.twitter.util.Return
import java.net.InetSocketAddress
import java.security.cert.{Certificate, X509Certificate}
import javax.net.ssl.{SSLEngine, SSLSession}
import org.jboss.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelPipeline,
  DownstreamChannelStateEvent
}
import org.jboss.netty.handler.ssl.SslHandler
import org.mockito.Matchers.any
import org.mockito.Mockito.{when, verify}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class ChannelTransportContextTest extends FunSuite with MockitoSugar {

  test("status is open when not failed and channel is not closed") {
    val ch = mock[Channel]
    when(ch.isOpen).thenReturn(true)
    val context = new ChannelTransportContext(ch)
    assert(context.status == Status.Open)
  }

  test("status is closed when failed") {
    val ch = mock[Channel]
    when(ch.isOpen).thenReturn(true)
    val context = new ChannelTransportContext(ch)
    context.failed.compareAndSet(false, true)
    assert(context.status == Status.Closed)
  }

  test("status is closed when channel is closed") {
    val ch = mock[Channel]
    when(ch.isOpen).thenReturn(false)
    val context = new ChannelTransportContext(ch)
    assert(context.status == Status.Closed)
  }

  test("onClose returns close promise") {
    val ch = mock[Channel]
    val context = new ChannelTransportContext(ch)
    assert(!context.onClose.isDefined)
    context.closep.updateIfEmpty(Return(new Exception("Hello")))
    assert(context.onClose.isDefined)
  }

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

  test("peer certificate is empty by default") {
    val ch = mock[Channel]
    val pipeline = mock[ChannelPipeline]
    when(ch.getPipeline).thenReturn(pipeline)
    when(pipeline.get(classOf[SslHandler])).thenReturn(null)
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
    val sslHandler = mock[SslHandler]
    when(sslHandler.getEngine).thenReturn(engine)

    val ch = mock[Channel]
    val pipeline = mock[ChannelPipeline]
    when(ch.getPipeline).thenReturn(pipeline)
    when(pipeline.get(classOf[SslHandler])).thenReturn(sslHandler)
    val context = new ChannelTransportContext(ch)
    assert(context.peerCertificate.nonEmpty)
  }

  test("close closes the channel") {
    val ch = mock[Channel]
    val pipeline = mock[ChannelPipeline]
    val closeFuture = mock[ChannelFuture]
    when(ch.isOpen).thenReturn(true)
    when(ch.getPipeline).thenReturn(pipeline)
    when(ch.getCloseFuture).thenReturn(closeFuture)
    val context = new ChannelTransportContext(ch)
    context.close(Time.now)
    verify(pipeline).sendDownstream(any[DownstreamChannelStateEvent])
    // If we reach here without bombing out, assume all is good.
    succeed
  }

}
