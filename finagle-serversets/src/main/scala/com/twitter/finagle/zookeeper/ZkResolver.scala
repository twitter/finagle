package com.twitter.finagle.zookeeper

import com.twitter.finagle.{Group, Resolver, InetResolver}
import com.twitter.common.zookeeper.{ServerSetImpl, ZooKeeperClient, ZooKeeperUtils}
import com.twitter.common.zookeeper.ServerSet
import com.twitter.util.{Future, Return, Throw, Try}
import java.net.{InetSocketAddress, SocketAddress}
import scala.collection.JavaConverters._
import com.twitter.common.net.pool.DynamicHostSet
import com.google.common.collect.ImmutableSet
import com.twitter.thrift.ServiceInstance
import com.twitter.thrift.Status.ALIVE

class ZkResolverException(msg: String) extends Exception(msg)

private class ZkGroup(serverSet: ServerSet, path: String)
    extends Thread("ZkGroup(%s)".format(path))
    with Group[ServiceInstance]
{
  setDaemon(true)
  start()

  @volatile private[this] var current: Set[ServiceInstance] = Set()
  def members = current

  override def run() {
    serverSet.monitor(new DynamicHostSet.HostChangeMonitor[ServiceInstance] {
      def onChange(newSet: ImmutableSet[ServiceInstance]) = synchronized {
        current = Set() ++ newSet.asScala
      }
    })
  }
}

class ZkResolver extends Resolver {
  val scheme = "zk"

  def resolve(zkHosts: Set[InetSocketAddress], path: String, endpoint: Option[String]) = {
    val group = endpoint match {
      case Some(endpoint) =>
        (new ZkGroup(new ServerSetImpl(ZkClient.get(zkHosts), path), path)) collect {
          case inst if inst.getStatus == ALIVE && inst.getAdditionalEndpoints.containsKey(endpoint) =>
            val ep = inst.getAdditionalEndpoints.get(endpoint)
            new InetSocketAddress(ep.getHost, ep.getPort): SocketAddress
        }

      case None =>
        (new ZkGroup(new ServerSetImpl(ZkClient.get(zkHosts), path), path)) collect {
          case inst if inst.getStatus == ALIVE =>
            val ep = inst.getServiceEndpoint
            new InetSocketAddress(ep.getHost, ep.getPort): SocketAddress
        }
    }
    Return(group)
  }

  private[this] def zkHosts(hosts: String) = {
    val zkHosts = ZkClient.hostSet(hosts)
    if (zkHosts.isEmpty)
      throw new ZkResolverException("ZK client address \"%s\" resolves to nothing".format(hosts))
    zkHosts
  }

  def resolve(addr: String) = addr.split("!") match {
    // zk!host:2181!/path
    case Array(hosts, path) =>
      resolve(zkHosts(hosts), path, None)

    // zk!host:2181!/path!endpoint
    case Array(hosts, path, endpoint) =>
      resolve(zkHosts(hosts), path, Some(endpoint))

    case _ =>
      Throw(new ZkResolverException("Invalid address \"%s\"".format(addr)))
  }
}
