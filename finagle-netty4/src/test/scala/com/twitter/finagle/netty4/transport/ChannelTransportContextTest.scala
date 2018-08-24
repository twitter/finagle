package com.twitter.finagle.netty4.transport

import com.twitter.finagle.Status
import com.twitter.util.{Return, Time}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import java.security.cert.{Certificate, X509Certificate}
import javax.net.ssl.{SSLEngine, SSLSession}
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.FunSuite

class ChannelTransportContextTest extends FunSuite with MockitoSugar {

  test("status is open when not failed and channel is not closed") {
    val ch = new EmbeddedChannel()
    assert(ch.isOpen)
    val context = new ChannelTransportContext(ch)
    assert(context.status == Status.Open)
  }

  test("status is closed when failed") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    context.failed.compareAndSet(false, true)
    assert(context.status == Status.Closed)
  }

  test("status is closed when channel is closed") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    ch.close()
    assert(context.status == Status.Closed)
  }

  test("onClose returns closed promise") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    assert(!context.onClose.isDefined)
    context.closed.updateIfEmpty(Return(new Exception("Hello")))
    assert(context.onClose.isDefined)
  }

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

  test("close closes the channel") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    context.close(Time.now)
    assert(!ch.isOpen)
  }

  test("calling close multiple times only closes the channel once") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    context.close(Time.now)
    assert(!ch.isOpen)
    assert(context.alreadyClosed.get)
    context.close(Time.now)
    context.close(Time.now)
    // Nothing bad happened
    succeed
  }

  test("executor is the channel's event loop") {
    val ch = new EmbeddedChannel()
    val context = new ChannelTransportContext(ch)
    assert(context.executor == ch.eventLoop)
  }

}
