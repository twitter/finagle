package com.twitter.finagle.channel

import org.specs.Specification
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.charset.Charset
import org.specs.mock.Mockito
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
        ReplyFuture.success(mock[Object])
      }
    }

    "before you connect" in {
      "writing throws an exception" in {
        val future = Channels.write(brokeredChannel, "yermom")
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
      var exceptionCaughtWasCalled = false
      brokeredChannel.getPipeline.addLast("handler", new SimpleChannelHandler {
        override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          channelConnectedWasCalled = true
        }

        override def channelBound(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
          channelBoundWasCalled = true
        }


        override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
          exceptionCaughtWasCalled = true
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

      "write" in {
        "dispatches" in {
          val future = Channels.write(brokeredChannel, "yermom")
          future.await()
          future.isSuccess must beTrue
        }

        "when writing twice" in {
          var dispatchCalledCount = 0
          val replyFuture = new ReplyFuture
          val connectFuture = brokeredChannel.connect(new Broker {
            def dispatch(e: MessageEvent) = {
              dispatchCalledCount += 1
              replyFuture
            }
          })
          Channels.write(brokeredChannel, "yermom")

          "an exception is raised" in {
            exceptionCaughtWasCalled must beFalse
            Channels.write(brokeredChannel, "yermom")
            exceptionCaughtWasCalled must beTrue
          }

          "dispatch is NOT called twice" in {
            Channels.write(brokeredChannel, "yermom")
            dispatchCalledCount mustBe 1
          }
        }
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
        val replyFuture = new ReplyFuture
        brokeredChannel.connect(new Broker {
          def dispatch(e: MessageEvent) = {
            e.getFuture.setSuccess()
            replyFuture
          }
        })

        Channels.write(brokeredChannel, "hey")
        replyFuture.isCancelled must beFalse
        Channels.close(brokeredChannel)
        replyFuture.isCancelled must beTrue
      }

      // XXX "on write success" in {
      // XXX   var writeCompletionFuture: ChannelFuture = null
      // XXX   val replyFuture = new ReplyFuture
      // XXX   brokeredChannel.connect(new Broker {
      // XXX     def dispatch(request: AnyRef) = {
      // XXX       // writeCompletionFuture = e.getFuture
      // XXX       replyFuture
      // XXX     }
      // XXX   })
      // XXX  
      // XXX   var writeCompleteWasCalled = false
      // XXX   var exceptionCaughtWasCalled = false
      // XXX   var messageReceivedWasCalled = false
      // XXX   brokeredChannel.getPipeline.addLast("handler", new SimpleChannelUpstreamHandler() {
      // XXX     override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
      // XXX       writeCompleteWasCalled = true
      // XXX     }
      // XXX  
      // XXX     override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      // XXX       exceptionCaughtWasCalled = true
      // XXX     }
      // XXX  
      // XXX     override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
      // XXX       messageReceivedWasCalled = true
      // XXX     }
      // XXX   })
      // XXX  
      // XXX   Channels.write(brokeredChannel, "hey")
      // XXX   Channels.close(brokeredChannel).await()
      // XXX   writeCompletionFuture.setSuccess()
      // XXX  
      // XXX   "the write complete event is not triggered" in {
      // XXX     writeCompleteWasCalled must beFalse
      // XXX   }
      // XXX  
      // XXX   "when the response is a success" in {
      // XXX     replyFuture.setSuccess()
      // XXX     messageReceivedWasCalled must beFalse
      // XXX   }
      // XXX  
      // XXX   "when the response fails" in {
      // XXX     replyFuture.setFailure(new Exception)
      // XXX     exceptionCaughtWasCalled must beFalse
      // XXX   }
      // XXX }
      // XXX  
      // XXX "on write failure the write exception event is not triggered" in {
      // XXX   var writeCompletionFuture: ChannelFuture = null
      // XXX   brokeredChannel.connect(new Broker {
      // XXX     def dispatch(request: AnyRef) = {
      // XXX       //writeCompletionFuture = e.getFuture
      // XXX       new ReplyFuture
      // XXX     }
      // XXX   })
      // XXX  
      // XXX   var exceptionCaughtWasCalled = false
      // XXX   brokeredChannel.getPipeline.addLast(
      // XXX     "observer", new SimpleChannelUpstreamHandler() {
      // XXX       override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      // XXX         exceptionCaughtWasCalled = true
      // XXX       }
      // XXX     }
      // XXX   )
      // XXX  
      // XXX   Channels.write(brokeredChannel, "hey")
      // XXX   Channels.close(brokeredChannel).await()
      // XXX   writeCompletionFuture.setFailure(new Exception)
      // XXX   exceptionCaughtWasCalled must beFalse
      // XXX }
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
