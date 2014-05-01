package com.twitter.finagle.socks

import com.twitter.util.RandomSocket
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.Arrays
import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.mockito.ArgumentCaptor
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.specs.matcher.Matcher
import com.twitter.finagle.ConnectionFailedException

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
    val port = RandomSocket.nextPort()
    val portByte1 = (port >> 8).toByte
    val portByte2 = (port & 0xFF).toByte

    val remoteAddress = new InetSocketAddress(InetAddress.getByAddress(null, Array[Byte](0x7F, 0x0, 0x0, 0x1)), port)
    channel.getRemoteAddress returns remoteAddress
    val proxyAddress = mock[SocketAddress]

    val connectFuture = Channels.future(channel, true)
    val connectRequested = new DownstreamChannelStateEvent(
        channel, connectFuture, ChannelState.CONNECTED, remoteAddress)

    def sendBytesToServer(x: Byte, xs: Byte*) {
      val ec = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
      there was atLeastOne(ctx).sendDownstream(ec.capture)
      val e = ec.getValue
      e.getMessage must matchBuffer(x, xs: _*)
    }

    def receiveBytesFromServer(ch: SocksConnectHandler, bytes: Array[Byte]) {
      ch.handleUpstream(ctx, new UpstreamMessageEvent(
        channel, ChannelBuffers.wrappedBuffer(bytes), null))
    }

    def connectAndRemoveHandler(ch: SocksConnectHandler) {
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

    def checkDidClose() {
      val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
      there was one(pipeline).sendDownstream(ec.capture)
      val e = ec.getValue
      e.getChannel must be(channel)
      e.getFuture must be(closeFuture)
      e.getState must be(ChannelState.OPEN)
      e.getValue must be(java.lang.Boolean.FALSE)
    }

    "with no authentication" in {

      val ch = new SocksConnectHandler(proxyAddress, remoteAddress)
      ch.handleDownstream(ctx, connectRequested)

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

      "when connect is successful" in {
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
            sendBytesToServer(0x05, 0x01, 0x00)
          }

          { // when init response is received send connect request
            receiveBytesFromServer(ch, Array[Byte](0x05, 0x00))

            sendBytesToServer(0x05, 0x01, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, portByte1, portByte2)
          }

          { // when connect response is received, propagate the connect and remove the handler
            receiveBytesFromServer(ch,
              Array[Byte](0x05, 0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, portByte1, portByte2))

            connectAndRemoveHandler(ch)
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
    "with username and password authentication" in {
      val username = "u"
      val password = "pass"
      val ch = new SocksConnectHandler(proxyAddress, remoteAddress,
        Seq(UsernamePassAuthenticationSetting(username, password)))
      ch.handleDownstream(ctx, connectRequested)

      "when connect is successful" in {
        ch.handleUpstream(ctx, new UpstreamChannelStateEvent(
          channel, ChannelState.CONNECTED, remoteAddress))
        connectFuture.isDone must beFalse
        there was no(ctx).sendUpstream(any)

        "do SOCKS negotiation" in {
          { // on connect send init
            sendBytesToServer(0x05, 0x01, 0x02)
          }

          { // when init response is received send user name and pass
            receiveBytesFromServer(ch, Array[Byte](0x05, 0x02))

            sendBytesToServer(0x01, 0x01, 0x75, 0x04, 0x70, 0x61, 0x73, 0x73)
          }

          { // when authenticated response is received send connect request
            receiveBytesFromServer(ch, Array[Byte](0x01, 0x00))

            sendBytesToServer(0x05, 0x01, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, portByte1, portByte2)
          }

          { // when connect response is received, propagate the connect and remove the handler
            receiveBytesFromServer(ch,
              Array[Byte](0x05, 0x00, 0x00, 0x01, 0x7F, 0x00, 0x00, 0x01, portByte1, portByte2))

            connectAndRemoveHandler(ch)
          }
        }

        "fail SOCKS negotiation when not authenticated" in {
          { // on connect send init
            sendBytesToServer(0x05, 0x01, 0x02)
          }

          { // when init response is received send user name and pass
            receiveBytesFromServer(ch, Array[Byte](0x05, 0x02))

            sendBytesToServer(0x01, 0x01, 0x75, 0x04, 0x70, 0x61, 0x73, 0x73)
          }

          { // when not authenticated response is received disconnect
            receiveBytesFromServer(ch, Array[Byte](0x01, 0x01))

            connectFuture.isDone must beTrue
            connectFuture.getCause must haveClass[ConnectionFailedException]
            checkDidClose
          }
        }
      }
    }
  }
}
