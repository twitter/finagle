package com.twitter.finagle.netty3.ssl.client

import com.twitter.finagle.{Address, SslVerificationFailedException}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientSessionVerifier}
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
    val address = mock[Address]
    val config = mock[SslClientConfiguration]
    val sessionVerifier = mock[SslClientSessionVerifier]
    when(sessionVerifier.apply(any[Address], any[SslClientConfiguration], any[SSLSession])) thenReturn true

    val connectFuture = Channels.future(channel, true)
    val connectRequested =
      new DownstreamChannelStateEvent(channel, connectFuture, ChannelState.CONNECTED, remoteAddress)

    val ch = new SslClientConnectHandler(sslHandler, address, config, sessionVerifier)
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
    ch.handleUpstream(
      ctx,
      new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, remoteAddress)
    )
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
  }

  test("SslClientConnectHandler when connect is successful should initiate a handshake") {
    val h = new helper2
    import h._

    verify(sslHandler).handshake()
  }

  test("SslClientConnectHandler when connect is successful should not propagate success") {
    val h = new helper2
    import h._

    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
  }

  test(
    "SslClientConnectHandler when connect is successful should propagate handshake failures as SslVerificationFailedException"
  ) {
    val h = new helper2
    import h._

    val exc = new Exception("sad panda")
    handshakeFuture.setFailure(exc)
    assert(connectFuture.isDone)
    assert(
      connectFuture.getCause ==
        new SslVerificationFailedException(exc, remoteAddress)
    )
  }

  test(
    "SslClientConnectHandler when connect is successful should propagate connection cancellation"
  ) {
    val h = new helper2
    import h._

    connectFuture.cancel()
    checkDidClose()
  }

  test("SslClientConnectHandler when handshake is successful should propagate success") {
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

  test("SslClientConnectHandler should apply session verification when handshake is successful") {
    val h = new helper2
    import h._

    verify(sessionVerifier, times(0))
      .apply(any[Address], any[SslClientConfiguration], any[SSLSession])
    handshakeFuture.setSuccess()
    verify(sessionVerifier, times(1))
      .apply(any[Address], any[SslClientConfiguration], any[SSLSession])
  }

  test("SslClientConnectHandler should close when session verification fails") {
    val h = new helper2
    import h._

    when(sessionVerifier(any[Address], any[SslClientConfiguration], any[SSLSession])) thenReturn false
    handshakeFuture.setSuccess()
    assert(connectFuture.isDone)
    assert(connectFuture.getCause.isInstanceOf[SslVerificationFailedException])
    checkDidClose()
  }

  test("SslClientConnectHandler should close when session verification throws") {
    val h = new helper2
    import h._

    val e = new RuntimeException("Failed verification")
    when(sessionVerifier(any[Address], any[SslClientConfiguration], any[SSLSession])) thenThrow e
    handshakeFuture.setSuccess()
    assert(connectFuture.isDone)
    assert(connectFuture.getCause.getMessage.startsWith("Failed verification"))
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
