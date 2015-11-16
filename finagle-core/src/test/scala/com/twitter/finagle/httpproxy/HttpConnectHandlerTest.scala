package com.twitter.finagle.httpproxy

import com.twitter.finagle.client.Transporter.Credentials
import java.net.{InetAddress, SocketAddress, InetSocketAddress}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when, atLeastOnce}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class HttpConnectHandlerTest extends FunSuite with MockitoSugar {
  class HttpConnectHandlerHelper {
    val ctx = mock[ChannelHandlerContext]
    val channel = mock[Channel]
    when(ctx.getChannel) thenReturn channel
    val pipeline = mock[ChannelPipeline]
    when(ctx.getPipeline) thenReturn pipeline
    when(channel.getPipeline) thenReturn pipeline
    val closeFuture = Channels.future(channel)
    when(channel.getCloseFuture) thenReturn closeFuture
    val remoteAddress = new InetSocketAddress(InetAddress.getLoopbackAddress, 80)
    val port = remoteAddress.getPort
    when(channel.getRemoteAddress) thenReturn remoteAddress
    val proxyAddress = mock[SocketAddress]
    val connectFuture = Channels.future(channel, true)
    val connectRequested = new DownstreamChannelStateEvent(
      channel, connectFuture, ChannelState.CONNECTED, remoteAddress)
    val ch = HttpConnectHandler.addHandler(proxyAddress, remoteAddress, pipeline, None)
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

  test("HttpConnectHandler should upon connect wrap the downstream connect request") {
    val h = new HttpConnectHandlerHelper
    import h._

    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue

    assert(e.getChannel == channel)
    assert(e.getFuture != connectFuture) // this is proxied
    assert(e.getState == ChannelState.CONNECTED)
    assert(e.getValue == proxyAddress)
  }

  test("HttpConnectHandler should upon connect propagate cancellation") {
    val h = new HttpConnectHandlerHelper
    import h._

    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue

    assert(!e.getFuture.isCancelled)
    connectFuture.cancel()
    assert(e.getFuture.isCancelled)
  }

  test("HttpConnectHandler should when connect is successful not propagate success") {
    val h = new HttpConnectHandlerHelper
    import h._

    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])
  }

  test("HttpConnectHandler should when connect is successful propagate connection cancellation") {
    val h = new HttpConnectHandlerHelper
    import h._

    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])

    connectFuture.cancel()
    checkDidClose()
  }

  test("HttpConnectHandler should when connect is successful do HTTP CONNECT") {
    val h = new HttpConnectHandlerHelper
    import h._

    ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])

    {
      // send connect request
      val ec = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
      verify(ctx, atLeastOnce).sendDownstream(ec.capture)
      val e = ec.getValue
      val req = e.getMessage.asInstanceOf[DefaultHttpRequest]
      assert(req.getMethod == HttpMethod.CONNECT)
      assert(req.getUri == "localhost:" + port)
      assert(req.headers().get("Host") == "localhost:" + port)
    }

    {
      // when connect response is received, propagate the connect and remove the handler
      ch.handleUpstream(ctx, new UpstreamMessageEvent(
        channel,
        new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK),
        null))

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
  }

  test("HttpConnectHandler should add ProxyAuthorization header when proxy credentials are supplied") {
    val h = new HttpConnectHandlerHelper
    import h._

    val handler = HttpConnectHandler.addHandler(
      proxyAddress,
      remoteAddress,
      pipeline,
      Some(Credentials("user", "pass")))

    handler.handleDownstream(ctx, connectRequested)
    handler.handleUpstream(ctx, new UpstreamChannelStateEvent(
      channel, ChannelState.CONNECTED, remoteAddress))
    assert(!connectFuture.isDone)
    verify(ctx, times(0)).sendUpstream(any[ChannelEvent])

    // send connect request
    val ec = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
    verify(ctx, atLeastOnce).sendDownstream(ec.capture)
    val e = ec.getValue
    val req = e.getMessage.asInstanceOf[DefaultHttpRequest]
    assert(req.getMethod == HttpMethod.CONNECT)
    assert(req.getUri == "localhost:" + port)
    assert(req.headers().get("Host") == "localhost:" + port)
    assert(req.headers().get("Proxy-Authorization") == "Basic dXNlcjpwYXNz")
  }

  test("HttpConnectHandler should propagate connection failure") {
    val h = new HttpConnectHandlerHelper
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
