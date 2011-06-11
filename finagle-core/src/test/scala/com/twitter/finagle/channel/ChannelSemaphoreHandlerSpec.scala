package com.twitter.finagle.channel

import scala.collection.JavaConversions._

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._

import com.twitter.concurrent.{AsyncSemaphore, Permit}
import com.twitter.util.{Promise, Return, Throw, Future}

import com.twitter.finagle._

object ChannelSemaphoreHandlerSpec extends Specification with Mockito {
  class FakeChannelHandlerContext extends ChannelHandlerContext {
    var _attachment: Object = null
    var _channel = mock[Channel]
    def getChannel = _channel
    def getPipeline = mock[ChannelPipeline]  //we mock this because it's queried by SimpleChannelHandler
    def getName = null
    def getHandler = null
    def canHandleUpstream = true
    def canHandleDownstream = true
    def sendUpstream(e: ChannelEvent) {  }
    def sendDownstream(e: ChannelEvent) {}
    def getAttachment = _attachment
    def setAttachment(attachment: Object) { _attachment = attachment }
  }

  def makeUpstreamMessage(ctx: ChannelHandlerContext) = {
    new UpstreamMessageEvent(ctx.getChannel, "message", null)
  }

  def makeDownstreamMessage(ctx: ChannelHandlerContext) = {
    new DownstreamMessageEvent(ctx.getChannel, Channels.future(ctx.getChannel, true),
      "message", null)
  }
  
  def makeUpstreamExceptionEvent(ctx: ChannelHandlerContext, cause: Throwable) =
    new DefaultExceptionEvent(ctx.getChannel, cause)

  "ChannelSemaphoreHandler" should {
    "enforce only a single request at a time" in {
      val semaphore = new AsyncSemaphore(1)
      val handler = new ChannelSemaphoreHandler(semaphore)

      "request, request should yield 1 upstream message" in {
        val ctx = spy(new FakeChannelHandlerContext)
        val event = makeUpstreamMessage(ctx)
        val event2 = makeUpstreamMessage(ctx)

        handler.messageReceived(ctx, event)
        handler.messageReceived(ctx, event2)

        there was one(ctx).sendUpstream(event)
        there was no(ctx).sendUpstream(event2)
      }

      "request, request, response should yield 2 upstream messages" in {
        val ctx = spy(new FakeChannelHandlerContext)
        val event = makeUpstreamMessage(ctx)
        val event2 = makeUpstreamMessage(ctx)

        handler.messageReceived(ctx, event)
        handler.messageReceived(ctx, event2)

        there was one(ctx).sendUpstream(event)
        there was no(ctx).sendUpstream(event2)

        val downstreamMessage = makeDownstreamMessage(ctx)
        handler.writeRequested(ctx, downstreamMessage)

        val proxiedDownstreamMessage = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
        there was one(ctx).sendDownstream(proxiedDownstreamMessage.capture())
        proxiedDownstreamMessage.getValue.getFuture.setSuccess

        there was one(ctx).sendUpstream(event2)
      }
    }
    
    "when the channel was closed or it excepted" in {
      val semaphore = new AsyncSemaphore(1)
      val handler = new ChannelSemaphoreHandler(semaphore)
      val ctx = spy(new FakeChannelHandlerContext)
      val event = makeUpstreamMessage(ctx)
      handler.messageReceived(ctx, event)
      val excEvent = makeUpstreamExceptionEvent(ctx, new Exception("wtf"))
      
      "propagates event and releases permit" in {
        val p = semaphore.acquire()
        p.isDefined must beFalse // the pending write has it
        handler.exceptionCaught(ctx, excEvent)
        there was one(ctx).sendUpstream(excEvent)
        p.isDefined must beTrue
      }

      "not throw an exception if we attempt to satisfy a pending write" in {
        there was one(ctx).sendUpstream(any[ChannelEvent])  // message
        handler.exceptionCaught(ctx, excEvent)
        there was one(ctx).sendUpstream(excEvent)
        there were two(ctx).sendUpstream(any[ChannelEvent])  // message + exc
        
        // i now try to write. this should not yield another exception.
        val downstreamMessage = makeDownstreamMessage(ctx)
        handler.writeRequested(ctx, downstreamMessage)
        
        // no additional events:
        there were two(ctx).sendUpstream(any[ChannelEvent])  // message + exc
      }
      
      "ignore messages after an exception" in {  // and permit is released
        handler.exceptionCaught(ctx, excEvent)
        there were two(ctx).sendUpstream(any[ChannelEvent])  // message + exc
        
        // attempt to send another message
        handler.messageReceived(ctx, makeUpstreamMessage(ctx))
        there were two(ctx).sendUpstream(any[ChannelEvent])
      }
    }
  }
}
