package com.twitter.finagle.netty3.ssl.client

import com.twitter.finagle.SslHandshakeException
import java.net.SocketAddress
import java.security.cert.Certificate
import javax.net.ssl.{SSLEngine, SSLSession}
import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class SslClientConnectHandlerTest extends FunSuite with MockitoSugar {

  class SslConnectHandlerHelper {
    val ctx = mock[ChannelHandlerContext]
    val sslHandler = mock[SslHandler]
    val session = mock[SSLSession]
    when(session.getPeerCertificates) thenReturn Array.empty[Certificate]
    val engine = mock[SSLEngine]
    when(engine.getSession) thenReturn session
    when(sslHandler.getEngine) thenReturn engine
    val channel = mock[Channel]
    when(ctx.getChannel) thenReturn channel
    val pipeline = mock[ChannelPipeline]
    when(channel.getPipeline) thenReturn pipeline
    val closeFuture = Channels.future(channel)
    when(channel.getCloseFuture) thenReturn closeFuture
    val remoteAddress = mock[SocketAddress]
    when(channel.getRemoteAddress) thenReturn remoteAddress

    val handshakeFuture = Channels.future(channel)
    when(sslHandler.handshake()) thenReturn handshakeFuture
  }

  class SslClientConnectHandlerHelper extends SslConnectHandlerHelper {
    val verifier = mock[SSLSession => Option[Throwable]]
    when(verifier(any[SSLSession])) thenReturn None

    val connectFuture = Channels.future(channel, true)
    val connectRequested = new DownstreamChannelStateEvent(
      channel, connectFuture, ChannelState.CONNECTED, remoteAddress)

    val ch = new SslClientConnectHandler(sslHandler, verifier)
    ch.handleDownstream(ctx, connectRequested)

    def checkDidClose() {
      val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
      verify(pipeline).sendDownstream(ec.capture)
      val e = ec.getValue
      assert(e.getChannel == channel)
      assert(e.getFuture == closeFuture)
      assert(e.getState == ChannelState.OPEN)
      assert(e.getValue == java.lang.Boolean.FALSE)
    }
  }

  test("SslClientConnectHandler should upon connect wrap the downstream connect request") {
    val h = new SslClientConnectHandlerHelper
    import h._

    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue

    assert(e.getChannel == channel)
    assert(e.getFuture != connectFuture) // this is proxied
    assert(e.getState == ChannelState.CONNECTED)
    assert(e.getValue == remoteAddress)
  }

  test("SslClientConnectHandler should upon connect propagate cancellation") {
    val h = new SslClientConnectHandlerHelper
    import h._

    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue

    assert(!e.getFuture.isCancelled)
    connectFuture.cancel()
    assert(e.getFuture.isCancelled)
  }

  class helper2 extends SslClientConnectHandlerHelper {
    verify(sslHandler, times(0)).handshake()
    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
  }

  test("SslClientConnectHandler should when connect is successful initiate a handshake") {
    val h = new helper2
    import h._

    verify(sslHandler).handshake()
  }

  test("SslClientConnectHandler should when connect is successful not propagate success") {
    val h = new helper2
    import h._

    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
  }

  test("SslClientConnectHandler should when connect is successful propagate handshake failures as SslHandshakeException") {
    val h = new helper2
    import h._

    val exc = new Exception("sad panda")
    handshakeFuture.setFailure(exc)
    assert(connectFuture.isDone)
    assert(connectFuture.getCause ==
      new SslHandshakeException(exc, remoteAddress))
  }

  test("SslClientConnectHandler should when connect is successful propagate connection cancellation") {
    val h = new helper2
    import h._

    connectFuture.cancel()
    checkDidClose()
  }

  test("SslClientConnectHandler should when connect is successful when handshake is successful propagate success") {
    val h = new helper2
    import h._

    handshakeFuture.setSuccess()
    assert(connectFuture.isDone)

    // we propagated the connect
    val ec = ArgumentCaptor.forClass(classOf[UpstreamChannelStateEvent])
    verify(ctx).sendUpstream(ec.capture)
    val e = ec.getValue

    assert(e.getChannel == channel)
    assert(e.getState == ChannelState.CONNECTED)
    assert(e.getValue == remoteAddress)
  }

  test("SslClientConnectHandler should when connect is successful when handshake is successful verify") {
    val h = new helper2
    import h._

    verify(verifier, times(0)).apply(any[SSLSession])
    handshakeFuture.setSuccess()
    verify(verifier).apply(any[SSLSession])
  }

  test("SslClientConnectHandler should when connect is successful when handshake is successful propagate verification failure") {
    val h = new helper2
    import h._

    val e = new Exception("session sucks")
    when(verifier(any[SSLSession])) thenReturn Some(e)
    handshakeFuture.setSuccess()
    assert(connectFuture.isDone)
    assert(connectFuture.getCause == e)
    checkDidClose()
  }

  test("SslClientConnectHandler should propagate connection failure") {
    val h = new SslClientConnectHandlerHelper
    import h._

    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue
    val exc = new Exception("failed to connect")

    assert(!connectFuture.isDone)
    e.getFuture.setFailure(exc)
    assert(connectFuture.isDone)
    assert(connectFuture.getCause == exc)
  }
}
