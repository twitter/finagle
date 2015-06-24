package com.twitter.finagle.netty3.channel

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import org.jboss.netty.channel.{
Channels, Channel, ChannelHandlerContext,
ChannelStateEvent, ChannelPipeline}

@RunWith(classOf[JUnitRunner])
class ChannelClosingHandlerTest extends FunSuite with MockitoSugar {

  class ChannelHelper {
    val channel = mock[Channel]
    val closeFuture = Channels.future(channel)
    when(channel.close()) thenReturn closeFuture
    val handler = new ChannelClosingHandler
    val ctx = mock[ChannelHandlerContext]
    val e = mock[ChannelStateEvent]
    val pipeline = mock[ChannelPipeline]

    when(pipeline.isAttached) thenReturn true

    when(ctx.getPipeline) thenReturn pipeline
    when(ctx.getChannel) thenReturn channel
  }

  test("ChannelClosingHandler should close the channel immediately when channel is already open") {
    val h = new ChannelHelper
    import h._

    handler.channelOpen(ctx, e)
    verify(channel, times(0)).close()
    handler.close()
    verify(channel, times(1)).close()
  }

  test("ChannelClosingHandler should close the channel immediately when channel is attached") {
    val h = new ChannelHelper
    import h._

    handler.beforeAdd(ctx)
    verify(channel, times(0)).close()
    handler.close()
    verify(channel, times(1)).close()
  }

  test("ChannelClosingHandler should delay closing until it has been opened before channel has been opened") {
    val h = new ChannelHelper
    import h._

    handler.close()
    verify(channel, times(0)).close()

    handler.channelOpen(ctx, e)
    verify(channel, times(1)).close()
  }

  test("ChannelClosingHandler should delay closing until it has been opened before channel has been attached") {
    val h = new ChannelHelper
    import h._

    handler.close()
    verify(channel, times(0)).close()

    handler.beforeAdd(ctx)
    verify(channel, times(1)).close()
  }

}
