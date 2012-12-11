package com.twitter.finagle

import com.twitter.finagle.builder.{Cluster, StaticCluster}
import com.twitter.finagle.util.InetSocketAddressUtil
import java.net.SocketAddress

/**
 * Clients connect to clusters.
 */
private[finagle]  // for now
trait Client[Req, Rep] {

  /**
   * Create a new `ServiceFactory` that is connected to `cluster`.
   */
  def newClient(cluster: Cluster[SocketAddress]): ServiceFactory[Req, Rep]

  // TODO: use actual resolver. The current implementation is but a
  // temporary convenience.
  def newClient(target: String): ServiceFactory[Req, Rep] = {
    val addrs = InetSocketAddressUtil.parseHosts(target)
    newClient(new StaticCluster[SocketAddress](addrs))
  }
}
