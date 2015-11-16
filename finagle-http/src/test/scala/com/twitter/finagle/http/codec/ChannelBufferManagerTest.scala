package com.twitter.finagle.http.codec

import com.twitter.finagle.ChannelBufferUsageException
import com.twitter.conversions.storage._
import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ChannelBufferManagerTest extends FunSuite with MockitoSugar {
  val me = mock[MessageEvent]
  val c = mock[Channel]
  val ctx = mock[ChannelHandlerContext]
  val e = mock[ChannelStateEvent]
  val wce = mock[WriteCompletionEvent]
  when(me.getChannel).thenReturn(c)

  def makeGetMessage(channelCapacity: Int): Unit = {
    val channelBuffer = ChannelBuffers.directBuffer(channelCapacity)
    doReturn(channelBuffer).when(me).getMessage
  }

  def usageTrackerFactory() = {
    val usageTracker = new ChannelBufferUsageTracker(1000.bytes)
    assert(usageTracker.usageLimit == (1000.bytes))
    usageTracker
  }
  def handlerFactory(usageTracker: ChannelBufferUsageTracker) = new ChannelBufferManager(usageTracker)

  test("track the capacity of the channel buffer") {
    val usageTracker = usageTrackerFactory()
    val handler = handlerFactory(usageTracker)

    makeGetMessage(256)
    handler.messageReceived(ctx, me)
    assert(usageTracker.currentUsage == (256.bytes))

    makeGetMessage(512)
    handler.messageReceived(ctx, me)
    assert(usageTracker.currentUsage == (768.bytes))

    handler.writeComplete(ctx, wce)
    usageTracker.currentUsage == (0.bytes)

    makeGetMessage(128)
    handler.messageReceived(ctx, me)
    usageTracker.currentUsage == (128.bytes)

    handler.channelClosed(ctx, e)
    usageTracker.currentUsage == (0.bytes)
    usageTracker.maxUsage == (768.bytes)
  }

  test("throw exception if usage exceeds limit at the beginning of the request") {
    val usageTracker = usageTrackerFactory()
    val handler = handlerFactory(usageTracker)

    usageTracker.setUsageLimit(10.bytes)
    assert(usageTracker.usageLimit == (10.bytes))
    assert(usageTracker.currentUsage == (0.bytes))

    makeGetMessage(20)
    intercept[ChannelBufferUsageException] {
      handler.messageReceived(ctx, me)
    }
    assert(usageTracker.currentUsage == (0.bytes))

    handler.channelClosed(ctx, e)
    assert(usageTracker.currentUsage == (0.bytes))
    assert(usageTracker.maxUsage == (0.bytes))
  }

  test("throw exception if usage exceeds limit in the middle of the request") {
    val usageTracker = usageTrackerFactory()
    val handler = handlerFactory(usageTracker)

    usageTracker.setUsageLimit(300.bytes)
    assert(usageTracker.usageLimit == (300.bytes))
    assert(usageTracker.currentUsage == (0.bytes))

    makeGetMessage(100)
    handler.messageReceived(ctx, me)
    assert(usageTracker.currentUsage == (100.bytes))

    makeGetMessage(350)
    intercept[ChannelBufferUsageException] {
      handler.messageReceived(ctx, me)
    }
    assert(usageTracker.currentUsage == (100.bytes))
    assert(usageTracker.maxUsage == (100.bytes))

    makeGetMessage(50)
    handler.messageReceived(ctx, me)
    assert(usageTracker.currentUsage == (150.bytes))
    assert(usageTracker.maxUsage == (150.bytes))

    makeGetMessage(150)
    handler.messageReceived(ctx, me)
    assert(usageTracker.currentUsage == (300.bytes))

    handler.channelClosed(ctx, e)
    assert(usageTracker.currentUsage == (0.bytes))
    assert(usageTracker.maxUsage == (300.bytes))
  }
}
