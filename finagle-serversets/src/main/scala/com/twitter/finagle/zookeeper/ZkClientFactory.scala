package com.twitter.finagle.zookeeper

import com.twitter.finagle.common.zookeeper.{ZooKeeperClient, ZooKeeperUtils}
import com.twitter.concurrent.{Offer, Broker, AsyncMutex}
import com.twitter.finagle.addr.StabilizingAddr.State._
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.util.Duration
import java.net.InetSocketAddress
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.{Watcher, WatchedEvent}
import scala.collection._
import scala.jdk.CollectionConverters._

private[finagle] class ZooKeeperHealthHandler extends Watcher {
  private[this] val mu = new AsyncMutex
  val pulse = new Broker[Health]()

  def process(evt: WatchedEvent) =
    for {
      permit <- mu.acquire()
      () <- evt.getState match {
        case KeeperState.SyncConnected => pulse ! Healthy
        case _ => pulse ! Unhealthy
      }
    } permit.release()
}

private[finagle] object DefaultZkClientFactory
    extends ZkClientFactory(ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT)

private[finagle] class ZkClientFactory(val sessionTimeout: Duration) {
  private[this] val zkClients: mutable.Map[Set[InetSocketAddress], ZooKeeperClient] = mutable.Map()

  def hostSet(hosts: String) = InetSocketAddressUtil.parseHosts(hosts).toSet

  def get(zkHosts: Set[InetSocketAddress]): (ZooKeeperClient, Offer[Health]) = synchronized {
    val client =
      zkClients.getOrElseUpdate(zkHosts, new ZooKeeperClient(sessionTimeout, zkHosts.asJava))

    // TODO: Watchers are tied to the life of the client,
    // which, in turn, is tied to the life of ZkClientFactory.
    // Maybe we should expose a way to unregister watchers.
    val healthHandler = new ZooKeeperHealthHandler
    client.register(healthHandler)
    (client, healthHandler.pulse.recv)
  }

  private[zookeeper] def clear() = synchronized { zkClients.clear() }
}
