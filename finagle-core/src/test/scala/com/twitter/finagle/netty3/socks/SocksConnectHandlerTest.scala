package com.twitter.finagle.netty3.socks

import com.twitter.finagle.ConnectionFailedException
import com.twitter.finagle.socks.UsernamePassAuthenticationSetting
import java.net.{SocketAddress, InetAddress, InetSocketAddress}
import java.util.Arrays
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel._
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when, atLeastOnce}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SocksConnectHandlerTest extends FunSuite with MockitoSugar {

  class SocksConnectHandlerHelper {
    val ctx = mock[ChannelHandlerContext]
    val channel = mock[Channel]
    when(ctx.getChannel) thenReturn channel
    val pipeline = mock[ChannelPipeline]
    when(ctx.getPipeline) thenReturn pipeline
    when(channel.getPipeline) thenReturn pipeline
    val closeFuture = Channels.future(channel)
    when(channel.getCloseFuture) thenReturn closeFuture
    val port = 80 // never bound
    val portByte1 = (port >> 8).toByte
    val portByte2 = (port & 0xFF).toByte

    val remoteAddress = new InetSocketAddress(InetAddress.getByAddress(null, Array[Byte](0x7F, 0x0, 0x0, 0x1)), port)
    when(channel.getRemoteAddress) thenReturn remoteAddress
    val proxyAddress = mock[SocketAddress]

    val connectFuture = Channels.future(channel, true)
    val connectRequested = new DownstreamChannelStateEvent(
      channel, connectFuture, ChannelState.CONNECTED, remoteAddress)

    def sendBytesToServer(x: Byte, xs: Byte*) {
      val ec = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
      verify(ctx, atLeastOnce()).sendDownstream(ec.capture)
      val e = ec.getValue
      assert(e.getMessage match {
        case buf: ChannelBuffer =>
          val a = Array(x, xs: _*)
          val bufBytes = Array.ofDim[Byte](buf.readableBytes())
          buf.getBytes(0, bufBytes)
          Arrays.equals(bufBytes, a)
      })
    }

    def receiveBytesFromServer(ch: SocksConnectHandler, bytes: Array[Byte]) {
      ch.handleUpstream(ctx, new UpstreamMessageEvent(
        channel, ChannelBuffers.wrappedBuffer(bytes), null))
    }

    def connectAndRemoveHandler(ch: SocksConnectHandler) {
      assert(connectFuture.isDone)
      verify(pipeline).remove(ch)

      // we propagated the connect
      val ec = ArgumentCaptor.forClass(classOf[UpstreamChannelStateEvent])
      verify(ctx).sendUpstream(ec.capture)
      val e = ec.getValue

      assert(e.getChannel == channel)
      assert(e.getState == ChannelState.CONNECTED)
      assert(e.getValue == remoteAddress)
    }

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

  test("SocksConnectHandler should with no authentication upon connect wrap the downstream connect request") {
    val h = new SocksConnectHandlerHelper
    import h._

    val ch = new SocksConnectHandler(proxyAddress, remoteAddress)
    ch.handleDownstream(ctx, connectRequested)
    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue

    assert(e.getChannel == channel)
    assert(e.getFuture != connectFuture) // this is proxied
    assert(e.getState == ChannelState.CONNECTED)
    assert(e.getValue == proxyAddress)

  }

  test("SocksConnectHandler should with no authentication upon connect propagate cancellation") {
    val h = new SocksConnectHandlerHelper
    import h._

    val ch = new SocksConnectHandler(proxyAddress, remoteAddress)
    ch.handleDownstream(ctx, connectRequested)
    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue

    assert(!e.getFuture.isCancelled)
    connectFuture.cancel()
    assert(e.getFuture.isCancelled)
  }

  test("SocksConnectHandler should with no authentication when connect is successful not propagate success") {
    val h = new SocksConnectHandlerHelper
    import h._

    val ch = new SocksConnectHandler(proxyAddress, remoteAddress)
    ch.handleDownstream(ctx, connectRequested)
    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])

    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
  }

  test("SocksConnectHandler should with no authentication when connect is successful propagate connection cancellation") {
    val h = new SocksConnectHandlerHelper
    import h._

    val ch = new SocksConnectHandler(proxyAddress, remoteAddress)
    ch.handleDownstream(ctx, connectRequested)
    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])

    connectFuture.cancel()
    checkDidClose()
  }

  test("SocksConnectHandler should with no authentication when connect is successful do SOCKS negotiation") {
    val h = new SocksConnectHandlerHelper
    import h._

    val ch = new SocksConnectHandler(proxyAddress, remoteAddress)
    ch.handleDownstream(ctx, connectRequested)
    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])

    {
      // on connect send init
      sendBytesToServer(0x05, 0x01, 0x00)
    }

    {
      // when init response is received send connect request
      receiveBytesFromServer(ch, Array[Byte](0x05, 0x00))

      sendBytesToServer(0x05, 0x01, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, portByte1, portByte2)
    }

    {
      // when connect response is received, propagate the connect and remove the handler
      receiveBytesFromServer(ch,
        Array[Byte](0x05, 0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, portByte1, portByte2))

      connectAndRemoveHandler(ch)
    }
  }

  test("SocksConnectHandler should with no authentication propagate connection failure") {
    val h = new SocksConnectHandlerHelper
    import h._

    val ch = new SocksConnectHandler(proxyAddress, remoteAddress)
    ch.handleDownstream(ctx, connectRequested)

    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue
    val exc = new Exception("failed to connect")

    assert(!connectFuture.isDone)
    e.getFuture.setFailure(exc)
    assert(connectFuture.isDone)
    assert(connectFuture.getCause == exc)
  }

  test("SocksConnectHandler should with username and password authentication when connect is successful do SOCKS negotiation") {
    val h = new SocksConnectHandlerHelper
    import h._

    val username = "u"
    val password = "pass"
    val ch = new SocksConnectHandler(proxyAddress, remoteAddress,
      Seq(UsernamePassAuthenticationSetting(username, password)))
    ch.handleDownstream(ctx, connectRequested)

    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])

    {
      // on connect send init
      sendBytesToServer(0x05, 0x01, 0x02)
    }

    {
      // when init response is received send user name and pass
      receiveBytesFromServer(ch, Array[Byte](0x05, 0x02))

      sendBytesToServer(0x01, 0x01, 0x75, 0x04, 0x70, 0x61, 0x73, 0x73)
    }

    {
      // when authenticated response is received send connect request
      receiveBytesFromServer(ch, Array[Byte](0x01, 0x00))

      sendBytesToServer(0x05, 0x01, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, portByte1, portByte2)
    }

    {
      // when connect response is received, propagate the connect and remove the handler
      receiveBytesFromServer(ch,
        Array[Byte](0x05, 0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, portByte1, portByte2))

      connectAndRemoveHandler(ch)
    }
  }

  test("SocksConnectHandler should with username and password authentication when connect is successful fail SOCKS negotiation when not authenticated") {
    val h = new SocksConnectHandlerHelper
    import h._

    val username = "u"
    val password = "pass"
    val ch = new SocksConnectHandler(proxyAddress, remoteAddress,
      Seq(UsernamePassAuthenticationSetting(username, password)))
    ch.handleDownstream(ctx, connectRequested)

    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])

    {
      // on connect send init
      sendBytesToServer(0x05, 0x01, 0x02)
    }

    {
      // when init response is received send user name and pass
      receiveBytesFromServer(ch, Array[Byte](0x05, 0x02))

      sendBytesToServer(0x01, 0x01, 0x75, 0x04, 0x70, 0x61, 0x73, 0x73)
    }

    {
      // when not authenticated response is received disconnect
      receiveBytesFromServer(ch, Array[Byte](0x01, 0x01))

      assert(connectFuture.isDone)
      assert(connectFuture.getCause.isInstanceOf[ConnectionFailedException])
      checkDidClose()
    }
  }
}
