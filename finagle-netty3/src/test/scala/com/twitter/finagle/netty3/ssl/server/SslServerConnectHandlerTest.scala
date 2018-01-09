package com.twitter.finagle.netty3.ssl.server

import com.twitter.finagle.Address
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerSessionVerifier}
import java.net.SocketAddress
import java.security.cert.Certificate
import javax.net.ssl.{SSLEngine, SSLSession}
import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class SslServerConnectHandlerTest extends FunSuite with MockitoSugar {

  class SslConnectHandlerHelper {
    val ctx = mock[ChannelHandlerContext]
    val sslHandler = mock[SslHandler]
    val sslSession = mock[SSLSession]
    when(sslSession.getPeerCertificates) thenReturn Array.empty[Certificate]
    val engine = mock[SSLEngine]
    when(engine.getSession) thenReturn sslSession
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

  class SslServerConnectHandlerHelper extends SslConnectHandlerHelper {
    var shutdownCount = 0
    def onShutdown() = shutdownCount += 1

    val config = mock[SslServerConfiguration]
    val verifier = mock[SslServerSessionVerifier]

    val connectHandler = new SslServerConnectHandler(sslHandler, config, verifier, onShutdown)
    val event = new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, remoteAddress)

    connectHandler.handleUpstream(ctx, event)
  }

  test("SslServerConnectHandler should call the shutdown callback on channel shutdown") {
    val h = new SslServerConnectHandlerHelper
    import h._

    val event = new UpstreamChannelStateEvent(channel, ChannelState.OPEN, null)
    connectHandler.channelClosed(mock[ChannelHandlerContext], event)
    assert(shutdownCount == 1)
  }

  test("SslServerConnectHandler should delay connection until the handshake is complete") {
    val h = new SslServerConnectHandlerHelper
    import h._

    when(verifier.apply(Address.failing, config, sslSession)) thenReturn true

    verify(sslHandler, times(1)).handshake()
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
    handshakeFuture.setSuccess()
    verify(ctx, times(1)).sendUpstream(any[ChannelEvent])
  }

  test("SslServerConnectHandler should not connect when verification fails") {
    val h = new SslServerConnectHandlerHelper
    import h._

    when(verifier.apply(Address.failing, config, sslSession)) thenReturn false

    verify(sslHandler, times(1)).handshake()
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
    handshakeFuture.setSuccess()
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
  }

  test("SslServerConnectHandler should not connect when verification throws") {
    val h = new SslServerConnectHandlerHelper
    import h._

    when(verifier.apply(Address.failing, config, sslSession)) thenThrow new RuntimeException("Failed verification")
    verify(sslHandler, times(1)).handshake()
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
    handshakeFuture.setSuccess()
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
  }

}
