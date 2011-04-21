package com.twitter.finagle.http

import java.nio.charset.Charset

import org.specs.Specification
import org.specs.mock.Mockito

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.ChannelBuffers

import com.twitter.finagle.ChannelBufferUsageException

object ChannelBufferManagerSpec extends Specification with Mockito {
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
    val usageTracker = new ChannelBufferUsageTracker(1000)
    val handler = new ChannelBufferManager(usageTracker)
    usageTracker.usageLimit must be_==(1000)

    "track the capacity of the channel buffer" in {
      makeGetMessage(256)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage() must be_==(256)

      makeGetMessage(512)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage() must be_==(768)

      handler.writeComplete(ctx, wce)
      usageTracker.currentUsage() must be_==(0)

      makeGetMessage(128)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage() must be_==(128)

      handler.channelClosed(ctx, e)
      usageTracker.currentUsage() must be_==(0)
      usageTracker.maxUsage() must be_==(768)
    }

    "throw exception if usage exceeds limit at the beginning of the request" in {
      usageTracker.setUsageLimit(10)
      usageTracker.usageLimit() must be_==(10)
      usageTracker.currentUsage() must be_==(0)

      makeGetMessage(20)
      handler.messageReceived(ctx, me) must throwA[ChannelBufferUsageException]
      usageTracker.currentUsage() must be_==(0)

      handler.channelClosed(ctx, e)
      usageTracker.currentUsage() must be_==(0)
      usageTracker.maxUsage() must be_==(0)
    }

    "throw exception if usage exceeds limit in the middle of the request" in {
      usageTracker.setUsageLimit(300)
      usageTracker.usageLimit() must be_==(300)
      usageTracker.currentUsage() must be_==(0)

      makeGetMessage(100)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage() must be_==(100)

      makeGetMessage(350)
      handler.messageReceived(ctx, me) must throwA[ChannelBufferUsageException]
      usageTracker.currentUsage() must be_==(100)
      usageTracker.maxUsage() must be_==(100)

      makeGetMessage(50)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage() must be_==(150)
      usageTracker.maxUsage() must be_==(150)

      makeGetMessage(150)
      handler.messageReceived(ctx, me)
      usageTracker.currentUsage() must be_==(300)

      handler.channelClosed(ctx, e)
      usageTracker.currentUsage() must be_==(0)
      usageTracker.maxUsage() must be_==(300)
    }
  }
}
