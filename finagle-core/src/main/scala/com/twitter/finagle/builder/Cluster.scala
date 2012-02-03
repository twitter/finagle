package com.twitter.finagle.builder
/*
 * Provides a class for specifying a collection of servers.
 * e.g. `finagle-serversets` is an implementation of the Finagle Cluster interface using
 * [[com.twitter.com.zookeeper.ServerSets] (
 * http://twitter.github.com/commons/apidocs/#com.twitter.common.zookeeper.ServerSet),
 * {{{
 *   val serverSet = new ServerSetImpl(zookeeperClient, "/twitter/services/silly")
 *   val cluster = new ZookeeperServerSetCluster(serverSet)
 * }}}
 */

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
   * Register a new Server in the cluster at the given SocketAddress, as so
   * {{{
   *   val serviceAddress = new InetSocketAddress(...)
   *   val server = ServerBuilder()
   *     .bindTo(serviceAddress)
   *     .build()
   *   cluster.join(serviceAddress)
   * }}}
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
