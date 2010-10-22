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
    val factory = mock[BrokeredChannelFactory]
    val pipeline = Channels.pipeline()
    pipeline.addLast("silenceWarnings", new SimpleChannelUpstreamHandler)
    val sink = new BrokeredChannelSink
    val brokeredChannel = new BrokeredChannel(factory, pipeline, sink)
    val defaultBroker = new Broker {
      def dispatch(e: MessageEvent) = {
        e.getFuture.setSuccess()
        UpcomingMessageEvent.successfulEvent(e.getChannel, mock[Object])
      }
    }


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

      val connectFuture = brokeredChannel.connect(defaultBroker)

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
        brokeredChannel.getRemoteAddress mustEqual defaultBroker
      }
    }


    "when the channel is closed" in {
      "the response event is cancelled" in {
        val responseEvent = new UpcomingMessageEvent(brokeredChannel)
        brokeredChannel.connect(new Broker {
          def dispatch(e: MessageEvent) = {
            e.getFuture.setSuccess()
            responseEvent
          }
        })

        Channels.write(brokeredChannel, "hey")
        responseEvent.getFuture.isCancelled must beFalse
        Channels.close(brokeredChannel)
        responseEvent.getFuture.isCancelled must beTrue
      }

      "on write success" in {
        var writeCompletionFuture: ChannelFuture = null
        val responseEvent = new UpcomingMessageEvent(brokeredChannel)
        brokeredChannel.connect(new Broker {
          def dispatch(e: MessageEvent) = {
            writeCompletionFuture = e.getFuture
            responseEvent
          }
        })

        var writeCompleteWasCalled = false
        var exceptionCaughtWasCalled = false
        var messageReceivedWasCalled = false
        brokeredChannel.getPipeline.addLast("handler", new SimpleChannelUpstreamHandler() {
          override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
            writeCompleteWasCalled = true
          }

          override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
            exceptionCaughtWasCalled = true
          }

          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
            messageReceivedWasCalled = true
          }
        })

        Channels.write(brokeredChannel, "hey")
        Channels.close(brokeredChannel).await()
        writeCompletionFuture.setSuccess()

        "the write complete event is not triggered" in {
          writeCompleteWasCalled must beFalse
        }

        "when the response is a success" in {
          responseEvent.getFuture.setSuccess()
          messageReceivedWasCalled must beFalse
        }

        "when the response fails" in {
          responseEvent.getFuture.setFailure(new Exception)
          exceptionCaughtWasCalled must beFalse
        }
      }

      "on write failure the write exception event is not triggered" in {
        var writeCompletionFuture: ChannelFuture = null
        brokeredChannel.connect(new Broker {
          def dispatch(e: MessageEvent) = {
            writeCompletionFuture = e.getFuture
            new UpcomingMessageEvent(e.getChannel)
          }
        })

        var exceptionCaughtWasCalled = false
        brokeredChannel.getPipeline.addLast(
          "observer", new SimpleChannelUpstreamHandler() {
            override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
              exceptionCaughtWasCalled = true
            }
          }
        )

        Channels.write(brokeredChannel, "hey")
        Channels.close(brokeredChannel).await()
        writeCompletionFuture.setFailure(new Exception)
        exceptionCaughtWasCalled must beFalse
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

      brokeredChannel.connect(defaultBroker).await(100, TimeUnit.MILLISECONDS)
      val closeFuture = brokeredChannel.close()

      "writing throws an exception" in {
        val future = Channels.write(brokeredChannel, ChannelBuffers.copiedBuffer("yermom", Charset.forName("UTF-8")))
        future.await()
        future.getCause must haveClass[NotYetConnectedException]
      }

      "future returns" in {
        closeFuture.await(100, TimeUnit.MILLISECONDS) must beTrue
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

      "isOpen becomes false" in {
        brokeredChannel.isOpen must beFalse
      }
    }

  }
}
