package com.twitter.finagle.http.codec


import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffers

import com.twitter.finagle.ChannelBufferUsageException
import com.twitter.conversions.storage._

class ChannelBufferManagerSpec extends SpecificationWithJUnit with Mockito {
  val me = mock[MessageEvent]
  val c = mock[Channel]
  val ctx = mock[ChannelHandlerContext]
  val e = mock[ChannelStateEvent]
  val wce = mock[WriteCompletionEvent]
  me.getChannel returns c

  def makeGetMessage(channelCapacity: Int) = {
    val channelBuffer = ChannelBuffers.directBuffer(channelCapacity)
    me.getMessage returns channelBuffer
  }

  "the channel buffer manager" should {
    val usageTracker = new ChannelBufferUsageTracker(1000.bytes)
    val handler = new ChannelBufferManager(usageTracker)
    usageTracker.usageLimit must be_==(1000.bytes)

    "track the capacity of the channel buffer" in {
      makeGetMessage(256)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage must be_==(256.bytes)

      makeGetMessage(512)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage must be_==(768.bytes)

      handler.writeComplete(ctx, wce)
      usageTracker.currentUsage must be_==(0.bytes)

      makeGetMessage(128)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage must be_==(128.bytes)

      handler.channelClosed(ctx, e)
      usageTracker.currentUsage must be_==(0.bytes)
      usageTracker.maxUsage must be_==(768.bytes)
    }

    "throw exception if usage exceeds limit at the beginning of the request" in {
      usageTracker.setUsageLimit(10.bytes)
      usageTracker.usageLimit must be_==(10.bytes)
      usageTracker.currentUsage must be_==(0.bytes)

      makeGetMessage(20)
      handler.messageReceived(ctx, me) must throwA[ChannelBufferUsageException]
      usageTracker.currentUsage must be_==(0.bytes)

      handler.channelClosed(ctx, e)
      usageTracker.currentUsage must be_==(0.bytes)
      usageTracker.maxUsage must be_==(0.bytes)
    }

    "throw exception if usage exceeds limit in the middle of the request" in {
      usageTracker.setUsageLimit(300.bytes)
      usageTracker.usageLimit must be_==(300.bytes)
      usageTracker.currentUsage must be_==(0.bytes)

      makeGetMessage(100)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage must be_==(100.bytes)

      makeGetMessage(350)
      handler.messageReceived(ctx, me) must throwA[ChannelBufferUsageException]
      usageTracker.currentUsage must be_==(100.bytes)
      usageTracker.maxUsage must be_==(100.bytes)

      makeGetMessage(50)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage must be_==(150.bytes)
      usageTracker.maxUsage must be_==(150.bytes)

      makeGetMessage(150)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage must be_==(300.bytes)

      handler.channelClosed(ctx, e)
      usageTracker.currentUsage must be_==(0.bytes)
      usageTracker.maxUsage must be_==(300.bytes)
    }
  }
}
