package com.twitter.finagle.channel

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.charset.Charset
import org.specs.mock.Mockito
import java.nio.channels.NotYetConnectedException
import java.util.concurrent.TimeUnit
import org.jboss.netty.channel._
import org.jboss.netty.channel.local.LocalAddress

object BrokeredChannelSpec extends Specification with Mockito {
  "BrokeredChannel" should {
    val broker = new Broker {
      def dispatch(handlingChannel: BrokeredChannel, e: MessageEvent) {
        e.getFuture.setSuccess()
      }
    }
    val factory = mock[BrokeredChannelFactory]
    val pipeline = Channels.pipeline()
    val sink = new BrokeredChannelSink
    val brokeredChannel = new BrokeredChannel(factory, pipeline, sink)

    "before you connect" in {
      "writing throws an exception" in {
        val future = Channels.write(brokeredChannel, ChannelBuffers.copiedBuffer("yermom", Charset.forName("UTF-8")))
        future.await()
        future.getCause must haveClass[NotYetConnectedException]
      }

      "getLocalAddress" in {
        brokeredChannel.getLocalAddress must beNull
      }

      "getRemoteAddress" in {
        brokeredChannel.getLocalAddress must beNull
      }

      "isConnected is false" in {
        brokeredChannel.isConnected must beFalse
      }

      "isConnected is false" in {
        brokeredChannel.isConnected must beFalse
      }
    }

    "when you are connected" in {
      var channelConnectedWasCalled = false
      var channelBoundWasCalled = false
      brokeredChannel.getPipeline.addLast("handler", new SimpleChannelHandler {
        override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          channelConnectedWasCalled = true
        }

        override def channelBound(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          channelBoundWasCalled = true
        }
      })

      val connectFuture = brokeredChannel.connect(broker)

      "the future returns" in {
        connectFuture.await(100, TimeUnit.MILLISECONDS) mustNot throwA[Exception]
      }

      "channelConnected handler triggers" in {
        channelConnectedWasCalled must beTrue
      }

      "channelBound handler triggers" in {
        channelBoundWasCalled must beTrue
      }


      "isConnected is true" in {
        brokeredChannel.isConnected must beTrue
      }

      "isBound is true" in {
        brokeredChannel.isBound must beTrue
      }

      "write dispatches" in {
        val future = Channels.write(brokeredChannel, ChannelBuffers.copiedBuffer("yermom", Charset.forName("UTF-8")))
        future.await()
        future.isSuccess must beTrue
      }

      "getLocalAddress is ephemeral" in {
        brokeredChannel.getLocalAddress.asInstanceOf[LocalAddress].isEphemeral must beTrue
      }

      "getRemoteAddress is the broker" in {
        brokeredChannel.getRemoteAddress mustEqual broker
      }
    }

    "when you disconnect" in {
      var channelClosedWasCalled = false
      var channelUnboundWasCalled = false
      var channelDisconnectedWasCalled = false
      brokeredChannel.getPipeline.addLast("handler", new SimpleChannelHandler {
        override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          channelClosedWasCalled = true
          super.channelClosed(ctx, e)
        }

        override def channelUnbound(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          channelUnboundWasCalled = true
          super.channelUnbound(ctx, e)
        }

        override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          channelDisconnectedWasCalled = true
          super.channelDisconnected(ctx, e)
        }
      })

      brokeredChannel.connect(broker).await(100, TimeUnit.MILLISECONDS)
      val closeFuture = brokeredChannel.close()

      "writing throws an exception" in {
        val future = Channels.write(brokeredChannel, ChannelBuffers.copiedBuffer("yermom", Charset.forName("UTF-8")))
        future.await()
        future.getCause must haveClass[NotYetConnectedException]
      }

      "future returns" in {
        closeFuture.await(100, TimeUnit.MILLISECONDS) mustNot throwA[Exception]
      }

      "channelClosed handler triggers" in {
        channelClosedWasCalled mustBe true
      }

      "channelUnbound handler triggers" in {
        channelUnboundWasCalled mustBe true
      }

      "channelDisconnectedWasCalled triggers" in {
        channelDisconnectedWasCalled mustBe true
      }

    }

  }
}