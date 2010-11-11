package com.twitter.finagle.http

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.finagle.channel.PartialUpstreamMessageEvent

import org.jboss.netty.channel.{
  Channel, ChannelHandlerContext, MessageEvent,
  SimpleChannelUpstreamHandler}
import org.jboss.netty.handler.codec.http.{
  HttpChunkTrailer, HttpResponse, HttpChunk}

object RequestLifecycleSpySpec extends Specification with Mockito {
  class SpyingSpy extends SimpleChannelUpstreamHandler  {
    var received: Option[MessageEvent] = None
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      received = Option(e)
    }
  }

  "RequestLifecycleSpy" should {
    val rls = new SpyingSpy with RequestLifecycleSpyBehavior
    val me = mock[MessageEvent]
    val c = mock[Channel]
    me.getChannel returns c
    val ctx = mock[ChannelHandlerContext]

    "wraps non-terminal chunked HttpResponse messages in PartialUpstreamMessageEvents" in {
      var r = mock[HttpResponse]
      r.isChunked returns true
      me.getMessage returns r
      rls.messageReceived(ctx, me)
      val Some(e: PartialUpstreamMessageEvent) = rls.received
      e mustNot beNull
    }

    "wraps non-terminal HttpChunk messages in PartialUpstreamMessageEvents" in {
      me.getMessage returns mock[HttpChunk]
      rls.messageReceived(ctx, me)
      val Some(e: PartialUpstreamMessageEvent) = rls.received
      e mustNot beNull
    }

    "forwards terminal HttpTrunkTrailers verbatim" in {
      me.getMessage returns mock[HttpChunkTrailer]
      rls.messageReceived(ctx, me)
      val e = rls.received.get.getMessage
      e mustNot beNull
      e.isInstanceOf[HttpChunkTrailer] must beTrue
    }

    "forwards all other messages verbatim" in {
      var r = mock[HttpResponse]
      r.isChunked returns false
      me.getMessage returns r
      rls.messageReceived(ctx, me)
      val e = rls.received.get.getMessage
      e mustNot beNull
      e.isInstanceOf[HttpResponse] must beTrue
    }
  }
}
