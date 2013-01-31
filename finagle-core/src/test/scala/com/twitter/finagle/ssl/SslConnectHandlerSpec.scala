package com.twitter.finagle.ssl

import com.twitter.finagle.SslHandshakeException
import java.net.SocketAddress
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLSession
import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import org.mockito.ArgumentCaptor
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class SslConnectHandlerSpec extends SpecificationWithJUnit with Mockito {
  "SslConnectHandler" should {
    val ctx = mock[ChannelHandlerContext]
    val sslHandler = mock[SslHandler]
    val session = mock[SSLSession]
    val engine = mock[SSLEngine]
    engine.getSession returns session
    sslHandler.getEngine returns engine
    val channel = mock[Channel]
    ctx.getChannel returns channel
    val pipeline = mock[ChannelPipeline]
    channel.getPipeline returns pipeline
    val closeFuture = Channels.future(channel)
    channel.getCloseFuture returns closeFuture
    val remoteAddress = mock[SocketAddress]
    channel.getRemoteAddress returns remoteAddress

    val verifier = mock[SSLSession => Option[Throwable]]
    verifier(any) returns None

    val connectFuture = Channels.future(channel, true)
    val connectRequested = new DownstreamChannelStateEvent(
        channel, connectFuture, ChannelState.CONNECTED, remoteAddress)

    val ch = new SslConnectHandler(sslHandler, verifier)
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
        e.getValue must be(remoteAddress)
      }

      "propagate cancellation" in {
        e.getFuture.isCancelled must beFalse
        connectFuture.cancel()
        e.getFuture.isCancelled must beTrue
      }
    }

    "when connect is succesful" in {
      there was no(sslHandler).handshake()
      val handshakeFuture = Channels.future(channel)
      sslHandler.handshake() returns handshakeFuture
      ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
        channel, ChannelState.CONNECTED, remoteAddress))
      connectFuture.isDone must beFalse
      there was no(ctx).sendUpstream(any)

      "initiate a handshake" in {
        there was one(sslHandler).handshake()
      }

      "not propagate success" in {
        there was no(ctx).sendUpstream(any)
      }

      "propagate handshake failures as SslHandshakeException" in {
         val exc = new Exception("sad panda")
          handshakeFuture.setFailure(exc)
          connectFuture.isDone must beTrue
          connectFuture.getCause must be_==(
            new SslHandshakeException(exc, remoteAddress))
      }

      "propagate connection cancellation" in {
        connectFuture.cancel()
        checkDidClose()
      }

      "when handshake is successful" in {
        "propagate success" in {
          handshakeFuture.setSuccess()
          connectFuture.isDone must beTrue

          // we propagated the connect
          val ec = ArgumentCaptor.forClass(classOf[UpstreamChannelStateEvent])
          there was one(ctx).sendUpstream(ec.capture)
          val e = ec.getValue

          e.getChannel must be(channel)
          e.getState must be(ChannelState.CONNECTED)
          e.getValue must be(remoteAddress)
        }

        "verify" in {
          there was no(verifier).apply(any)
          handshakeFuture.setSuccess()
          there was one(verifier).apply(any)
        }

        "propagate verification failure" in {
          val e = new Exception("session sucks")
          verifier(any) returns Some(e)
          handshakeFuture.setSuccess()
          connectFuture.isDone must beTrue
          connectFuture.getCause must be(e)
          checkDidClose()
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
