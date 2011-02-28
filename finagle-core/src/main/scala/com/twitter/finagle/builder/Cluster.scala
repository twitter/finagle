package com.twitter.finagle.builder

import java.net.SocketAddress
import com.twitter.finagle.ServiceFactory

/**
 * A collection of SocketAddresses. The intention of this interface
 * to express membership in a cluster of servers that provide a
 * specific service.
 *
 * Note that a Cluster can be elastic: members can join or leave at
 * any time.
 */
trait Cluster {
  /**
   * Produce a sequence of ServiceFactories that changes as servers join and
   * leave the cluster.
   */
  def mkFactories[Req, Rep](f: SocketAddress => ServiceFactory[Req, Rep]): Seq[ServiceFactory[Req, Rep]]

  /**
   * Register a new Server in the cluster at the given SocketAddress.
   */
  def join(address: SocketAddress)
}

class SocketAddressCluster(underlying: Seq[SocketAddress])
  extends Cluster
{
  private[this] var self = underlying

  def mkFactories[Req, Rep](f: SocketAddress => ServiceFactory[Req, Rep]) = self map f

  def join(address: SocketAddress) {
    self = underlying ++ Seq(address)
  }
}
