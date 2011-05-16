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
    def getPipeline = null
    def getName = null
    def getHandler = null
    def canHandleUpstream = true
    def canHandleDownstream = true
    def sendUpstream(e: ChannelEvent) {}
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
  }
}
