package com.twitter.finagle.socks

import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.Arrays
import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.mockito.ArgumentCaptor
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.specs.matcher.Matcher

class SocksConnectHandlerSpec extends SpecificationWithJUnit with Mockito {
  case class matchBuffer(x: Byte, xs: Byte*) extends Matcher[AnyRef] {
    val a = Array(x, xs : _*)

    def apply(v: => AnyRef) = {
      v match {
        case (buf: ChannelBuffer) =>
          val bufBytes = Array.ofDim[Byte](buf.readableBytes())
          buf.getBytes(0, bufBytes)
          val bufBytesToString = Arrays.toString(bufBytes)
          val aToString = Arrays.toString(a)
          (Arrays.equals(bufBytes, a),
           bufBytesToString + " matches " + aToString,
           bufBytesToString + " does not match " + aToString)
        case _ => (false, "?", "not a HeapChannelBuffer")
      }
    }
  }

  "SocksConnectHandler" should {
    val ctx = mock[ChannelHandlerContext]
    val channel = mock[Channel]
    ctx.getChannel returns channel
    val pipeline = mock[ChannelPipeline]
    ctx.getPipeline returns pipeline
    channel.getPipeline returns pipeline
    val closeFuture = Channels.future(channel)
    channel.getCloseFuture returns closeFuture
    val remoteAddress = new InetSocketAddress(InetAddress.getByAddress(null, Array[Byte](0x7F, 0x0, 0x0, 0x1)), 0x7F7F)
    channel.getRemoteAddress returns remoteAddress
    val proxyAddress = mock[SocketAddress]

    val connectFuture = Channels.future(channel, true)
    val connectRequested = new DownstreamChannelStateEvent(
        channel, connectFuture, ChannelState.CONNECTED, remoteAddress)

    val ch = new SocksConnectHandler(proxyAddress, remoteAddress)
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

      "do SOCKS negotiation" in {
        { // on connect send init
          val ec = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
          there was atLeastOne(ctx).sendDownstream(ec.capture)
          val e = ec.getValue
          e.getMessage must matchBuffer(0x05, 0x01, 0x00)
        }

        { // when init response is received send connect request
          ch.handleUpstream(ctx, new UpstreamMessageEvent(
            channel, ChannelBuffers.wrappedBuffer(Array[Byte](0x05, 0x00)), null))

          val ec = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
          there was atLeastOne(ctx).sendDownstream(ec.capture)
          val e = ec.getValue
          e.getMessage must matchBuffer(0x05, 0x01, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, 0x7F, 0x7F)
        }

        { // when connect response is received, propagate the connect and remove the handler
          ch.handleUpstream(ctx, new UpstreamMessageEvent(
            channel,
            ChannelBuffers.wrappedBuffer(Array[Byte](0x05, 0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, 0x7F, 0x7F)),
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
