package com.twitter.finagle.zookeeper

import com.twitter.common.quantity.{Amount, Time => CommonTime}
import com.twitter.common.zookeeper.{ZooKeeperClient, ZooKeeperUtils}
import com.twitter.concurrent.{Offer, Broker, AsyncMutex}
import com.twitter.finagle.addr.StabilizingAddr.State._
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.util.Duration
import java.lang.{Long => JLong}
import java.net.InetSocketAddress
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.{Watcher, WatchedEvent}
import scala.collection.JavaConverters._
import scala.collection._

private[finagle] class ZooKeeperHealthHandler extends Watcher {
  private[this] val mu = new AsyncMutex
  val pulse = new Broker[Health]()

  def process(evt: WatchedEvent) = for {
    permit <- mu.acquire()
    () <- evt.getState match {
      case KeeperState.SyncConnected => pulse ! Healthy
      case _ => pulse ! Unhealthy
    }
  } permit.release()
}

private[zookeeper] object DurationHelper {
  private[finagle] def convertToDuration(amt: Amount[JLong, CommonTime]): Duration =
    Duration(amt.getValue.longValue, amt.getUnit.getTimeUnit)
}

private[finagle] object DefaultZkClientFactory
  extends ZkClientFactory(DurationHelper.convertToDuration(Amount.of(
    ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT.getValue.toLong,
    ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT.getUnit)))

private[finagle] class ZkClientFactory(val sessionTimeout: Duration) {
  private[this] val zkClients: mutable.Map[Set[InetSocketAddress], ZooKeeperClient] = mutable.Map()

  def hostSet(hosts: String) = InetSocketAddressUtil.parseHosts(hosts).toSet

  def get(zkHosts: Set[InetSocketAddress]): (ZooKeeperClient, Offer[Health]) = synchronized {
    val client = zkClients.getOrElseUpdate(zkHosts, new ZooKeeperClient(
        Amount.of(sessionTimeout.inMillis.toInt, CommonTime.MILLISECONDS),
        zkHosts.asJava))
    // TODO: Watchers are tied to the life of the client,
    // which, in turn, is tied to the life of ZkClientFactory.
    // Maybe we should expose a way to unregister watchers.
    val healthHandler = new ZooKeeperHealthHandler
    client.register(healthHandler)
    (client, healthHandler.pulse.recv)
  }

  private[zookeeper] def clear() = synchronized { zkClients.clear() }
}
