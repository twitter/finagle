package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito

import org.jboss.netty.channel.{
  Channels, Channel, ChannelFuture, ChannelHandlerContext,
  ChannelStateEvent, ChannelPipeline}

import com.twitter.finagle.health._
import com.twitter.finagle.stats.NullStatsReceiver

import com.twitter.util.Promise

object ChannelOpenConnectionsHandlerSpec extends Specification with Mockito {
  "ChannelOpenConnectionsHandlerSpec" should {
    def open(handler: ConnectionLifecycleHandler): ChannelFuture = {
      val channel = mock[Channel]

      val closeFuture = Channels.future(channel)
      channel.getCloseFuture() returns closeFuture

      val ctx = mock[ChannelHandlerContext]
      ctx.getChannel returns channel

      val e = mock[ChannelStateEvent]

      handler.channelOpen(ctx, e)
      closeFuture
    }

    val callback = mock[HealthEvent => Unit]

    val thresholds = OpenConnectionsHealthThresholds(15, 10)

    val handler = new ChannelOpenConnectionsHandler(thresholds, callback, NullStatsReceiver)
    val onClose = new Promise[Unit]

    "increments and decrements" in {
      handler.openConnections mustEqual 0
      val closeFuture = open(handler)
      handler.openConnections mustEqual 1
      closeFuture.setSuccess()
      handler.openConnections mustEqual 0
    }

    "marks unhealthy and healthy" in {
      handler.openConnections mustEqual 0
      handler.isHealthy mustEqual true

      // load up
      val closeFutures =
        for (i <- 1 to thresholds.highWaterMark) yield {
          val closeFuture = open(handler)
          handler.openConnections mustEqual i
          handler.isHealthy mustEqual true
          closeFuture
        }

      // push above high-water mark
      there were no(callback).apply(any[Unhealthy])
      val closeFuture = open(handler)
      handler.isHealthy mustEqual false
      handler.openConnections mustEqual thresholds.highWaterMark + 1
      there was one(callback).apply(Unhealthy(UnhealthyReason.TooManyOpenConnections))

      // drain
      for (i <- (thresholds.lowWaterMark to thresholds.highWaterMark).reverse) {
        closeFutures(i-1).setSuccess()
        handler.openConnections mustEqual i
        handler.isHealthy mustEqual false
      }

      // push below low-water mark
      there were no(callback).apply(Healthy())
      closeFuture.setSuccess()
      handler.openConnections mustEqual thresholds.lowWaterMark - 1
      handler.isHealthy mustEqual true

      there was one(callback).apply(Healthy())
      there was one(callback).apply(Unhealthy(UnhealthyReason.TooManyOpenConnections))
    }
  }
}
