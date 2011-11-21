package com.twitter.finagle.channel

import org.jboss.netty.channel.{SimpleChannelHandler, ChannelHandlerContext}

import com.twitter.finagle.health.{HealthEvent, Healthy, Unhealthy, UnhealthyReason}
import com.twitter.finagle.stats.StatsReceiver

import com.twitter.util.Future

/**
 * A HealthHandler for open connections, based on high and low water marks.
 * If the unhealthyThreshold is reached, mark the server as unhealthy until
 * the healthThreshold is reached.
 */
case class OpenConnectionsHealthThresholds(highWaterMark: Int, lowWaterMark: Int) {
  require(highWaterMark > lowWaterMark, "highWaterMark must be > lowWaterMark")
}

/**
 * keeps track of open connections and invokes the OpenConnectionsHealthCallback
 * when server transitions between healthy and unhealthy
 */
class ChannelOpenConnectionsHandler(
    thresholds: OpenConnectionsHealthThresholds,
    callback: HealthEvent => Unit,
    statsReceiver: StatsReceiver)
  extends SimpleChannelHandler
  with ConnectionLifecycleHandler
{
  private[this] var connectionCount = 0
  private[this] var healthy = true

  def openConnections = connectionCount
  def isHealthy = healthy

  private[this] val (connectionsGauge, healthyGauge) = {
    val scoped = statsReceiver.scope("open_connections_handler")
    (
      scoped.addGauge("connections") { openConnections },
      scoped.addGauge("healthy") { if (isHealthy) 1F else 0F}
    )
  }

  protected def channelConnected(ctx: ChannelHandlerContext, onClose: Future[Unit]) {
    val event = synchronized {
      connectionCount += 1
      // if unhealthy, and this is a state transition from healthy, call makeUnhealthy
      if (healthy && connectionCount > thresholds.highWaterMark) {
        healthy = false
        Some(Unhealthy(UnhealthyReason.TooManyOpenConnections))
      } else {
        None
      }
    }
    event foreach { callback(_) }

    onClose ensure {
      val event = synchronized {
        connectionCount -= 1
        // if healthy, and this is a state transition from unhealthy, call makeHealthy
        if (!healthy && connectionCount < thresholds.lowWaterMark) {
          healthy = true
          Some(Healthy())
        } else {
          None
        }
      }
      event foreach { callback(_) }
    }
  }
}