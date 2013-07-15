package com.twitter.finagle.zookeeper

import com.twitter.common.zookeeper.{ZooKeeperClient, ZooKeeperUtils}
import com.twitter.finagle.InetResolver
import java.net.InetSocketAddress
import scala.collection.JavaConverters._

private[finagle] object ZkClient {
  private[this] var zkClients: Map[Set[InetSocketAddress], ZooKeeperClient] = Map()

  def hostSet(hosts: String) = {
    val zkGroup = InetResolver.resolve(hosts)() collect { case ia: InetSocketAddress => ia }
    zkGroup()
  }

  def get(zkHosts: Set[InetSocketAddress]) = synchronized {
    if (!(zkClients contains zkHosts)) {
      val newZk = new ZooKeeperClient(
        ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT,
        zkHosts.asJava)
      zkClients += zkHosts -> newZk
    }

    zkClients(zkHosts)
  }

  private[zookeeper] def clear() = synchronized { zkClients = Map() }
}
