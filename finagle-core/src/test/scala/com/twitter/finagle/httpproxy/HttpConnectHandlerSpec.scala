package com.twitter.finagle.httpproxy

import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.Arrays
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, DefaultHttpResponse, HttpMethod,
  HttpResponseStatus, HttpVersion}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.mockito.ArgumentCaptor
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.specs.matcher.Matcher

class HttpConnectHandlerSpec extends SpecificationWithJUnit with Mockito {
  "HttpConnectHandler" should {
    val ctx = mock[ChannelHandlerContext]
    val channel = mock[Channel]
    ctx.getChannel returns channel
    val pipeline = mock[ChannelPipeline]
    ctx.getPipeline returns pipeline
    channel.getPipeline returns pipeline
    val closeFuture = Channels.future(channel)
    channel.getCloseFuture returns closeFuture
    val remoteAddress = new InetSocketAddress("localhost", 443)
    channel.getRemoteAddress returns remoteAddress
    val proxyAddress = mock[SocketAddress]
    val connectFuture = Channels.future(channel, true)
    val connectRequested = new DownstreamChannelStateEvent(
        channel, connectFuture, ChannelState.CONNECTED, remoteAddress)
    val ch = HttpConnectHandler.addHandler(proxyAddress, remoteAddress, pipeline)
    ch.handleDownstream(ctx, connectRequested)

    def checkDidClose() {
      val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
      there was one(pipeline).sendDownstream(ec.capture)
      val e = ec.getValue
      e.getChannel must be(channel)
      e.getFuture must be(closeFuture)
      e.getState must be(ChannelState.OPEN)
      e.getValue must be(java.lang.Boolean.FALSE)
    }

    "upon connect" in {
      val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
      there was one(ctx).sendDownstream(ec.capture)
      val e = ec.getValue

      "wrap the downstream connect request" in {
        e.getChannel must be(channel)
        e.getFuture must notBe(connectFuture)  // this is proxied
        e.getState must be(ChannelState.CONNECTED)
        e.getValue must be(proxyAddress)
      }

      "propagate cancellation" in {
        e.getFuture.isCancelled must beFalse
        connectFuture.cancel()
        e.getFuture.isCancelled must beTrue
      }
    }

    "when connect is succesful" in {
      ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
        channel, ChannelState.CONNECTED, remoteAddress))
      connectFuture.isDone must beFalse
      there was no(ctx).sendUpstream(any)

      "not propagate success" in {
        there was no(ctx).sendUpstream(any)
      }

      "propagate connection cancellation" in {
        connectFuture.cancel()
        checkDidClose()
      }

      "do HTTP CONNECT" in {
        { // send connect request
          val ec = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
          there was atLeastOne(ctx).sendDownstream(ec.capture)
          val e = ec.getValue
          val req = e.getMessage.asInstanceOf[DefaultHttpRequest]
          req.getMethod must_== HttpMethod.CONNECT
          req.getUri must_== "localhost:443"
        }

        { // when connect response is received, propagate the connect and remove the handler
          ch.handleUpstream(ctx, new UpstreamMessageEvent(
            channel,
            new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK),
            null))

          connectFuture.isDone must beTrue
          there was one(pipeline).remove(ch)

          // we propagated the connect
          val ec = ArgumentCaptor.forClass(classOf[UpstreamChannelStateEvent])
          there was one(ctx).sendUpstream(ec.capture)
          val e = ec.getValue

          e.getChannel must be(channel)
          e.getState must be(ChannelState.CONNECTED)
          e.getValue must be(remoteAddress)
        }
      }
    }

    "propagate connection failure" in {
      val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
      there was one(ctx).sendDownstream(ec.capture)
      val e = ec.getValue
      val exc = new Exception("failed to connect")

      connectFuture.isDone must beFalse
      e.getFuture.setFailure(exc)
      connectFuture.isDone must beTrue
      connectFuture.getCause must be_==(exc)
    }
  }
}
