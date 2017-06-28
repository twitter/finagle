package com.twitter.finagle.netty3.channel

import com.twitter.finagle.stats.InMemoryStatsReceiver
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.channel.{ChannelHandlerContext, ChannelStateEvent, MessageEvent}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ChannelRequestStatsHandlerTest extends FunSuite with MockitoSugar {
  test("records requests number") {
    val sr = new InMemoryStatsReceiver
    val handler = new ChannelRequestStatsHandler(sr)

    val ctx = mock[ChannelHandlerContext]
    val cnt = new AtomicInteger(0)
    val e = mock[ChannelStateEvent]
    val msg = mock[MessageEvent]

    assert(sr.stats.get(Seq("connection_requests")) == None)
    handler.channelOpen(ctx, e)

    when(ctx.getAttachment.asInstanceOf[AtomicInteger]).thenReturn(cnt)
    handler.messageReceived(ctx, msg)
    handler.messageReceived(ctx, msg)
    assert(sr.stats.get(Seq("connection_requests")) == None)

    handler.channelClosed(ctx, e)
    assert(sr.stats(Seq("connection_requests")) == Seq(2.0f))
  }
}
