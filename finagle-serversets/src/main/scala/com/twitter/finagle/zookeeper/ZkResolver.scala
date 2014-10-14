package com.twitter.finagle.zookeeper

import com.google.common.collect.ImmutableSet
import com.twitter.common.net.pool.DynamicHostSet
import com.twitter.common.net.pool.DynamicHostSet.MonitorException
import com.twitter.common.zookeeper.{ServerSet, ServerSetImpl}
import com.twitter.concurrent.{Broker, Offer}
import com.twitter.finagle.addr.StabilizingAddr
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.{Group, Resolver, Addr}
import com.twitter.thrift.Endpoint
import com.twitter.thrift.ServiceInstance
import com.twitter.util.Var
import java.net.{InetSocketAddress, SocketAddress}
import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.logging.{Level, Logger}

/**
 * Indicates that a failure occurred while attempting to resolve a cluster
 * using a [[com.twitter.finagle.zookeeper.ZkAnnouncer]].
 */
class ZkResolverException(msg: String) extends Exception(msg)

// Note: this is still used by finagle-memcached.
private[finagle] class ZkGroup(serverSet: ServerSet, path: String)
    extends Thread("ZkGroup(%s)".format(path))
    with Group[ServiceInstance]
{
  setDaemon(true)
  start()

  protected[finagle] val set = Var(Set[ServiceInstance]())

  override def run() {
    serverSet.watch(new DynamicHostSet.HostChangeMonitor[ServiceInstance] {
      def onChange(newSet: ImmutableSet[ServiceInstance]) = synchronized {
        set() = newSet.asScala.toSet
      }
    })
  }
}


private class ZkOffer(serverSet: ServerSet, path: String)
    extends Thread("ZkOffer(%s)".format(path)) with Offer[Set[ServiceInstance]] {
  setDaemon(true)
  start()

  private[this] val inbound = new Broker[Set[ServiceInstance]]
  private[this] val log = Logger.getLogger("zkoffer")

  override def run() {
    try {
      serverSet.watch(new DynamicHostSet.HostChangeMonitor[ServiceInstance] {
        def onChange(newSet: ImmutableSet[ServiceInstance]) {
          inbound !! newSet.asScala.toSet
        }
      })
    } catch {
      case exc: MonitorException =>
        // There are certain path permission checks in the serverset library
        // that can cause exceptions here. We'll send an empty set (which
        // becomes a negative resolution).
        log.log(Level.WARNING, "Exception when trying to watch a ServerSet! " +
          "Returning negative resolution.", exc)
        inbound !! Set.empty
    }
  }

  def prepare() = inbound.recv.prepare()
}


class ZkResolver(factory: ZkClientFactory) extends Resolver {
  val scheme = "zk"

  // With the current serverset client, instances are maintained
  // forever; additional resource leaks aren't created by caching
  // instances here.
  private type CacheKey = (Set[InetSocketAddress], String, Option[String], Option[Int])
  private val cache = new mutable.HashMap[CacheKey, Var[Addr]]

  def this() = this(DefaultZkClientFactory)

  def resolve(
      zkHosts: Set[InetSocketAddress],
      path: String,
      endpoint: Option[String] = None,
      shardId: Option[Int] = None): Var[Addr] =
    synchronized {
      cache.getOrElseUpdate(
        (zkHosts, path, endpoint, shardId),
        newVar(zkHosts, path, newToSocketAddrs(endpoint, shardId)))
    }

  private def newToSocketAddrs(endpoint: Option[String], shardId: Option[Int]) = {

    val getEndpoint: PartialFunction[ServiceInstance, Endpoint] = endpoint match {
      case Some(epname) => {
        case inst if inst.getAdditionalEndpoints.containsKey(epname) =>
          inst.getAdditionalEndpoints.get(epname)
      }
      case None => {
        case inst: ServiceInstance => inst.getServiceEndpoint()
      }
    }

    val filterShardId: PartialFunction[ServiceInstance, ServiceInstance] = shardId match {
      case Some(id) => {
        case inst if inst.isSetShard && inst.shard == id => inst
      }
      case None => { case x => x }
    }

    val toSocketAddr: Endpoint => SocketAddress = (ep: Endpoint) =>
      new InetSocketAddress(ep.getHost, ep.getPort)

    (insts: Set[ServiceInstance]) =>
      insts.collect(filterShardId).collect(getEndpoint).map(toSocketAddr)
  }

  private def newVar(
      zkHosts: Set[InetSocketAddress],
      path: String, toSocketAddrs: Set[ServiceInstance] => Set[SocketAddress]) = {

    val (zkClient, zkHealthHandler) = factory.get(zkHosts)
    val zkOffer = new ZkOffer(new ServerSetImpl(zkClient, path), path)
    val addrOffer = zkOffer map { newSet =>
      val sockaddrs = toSocketAddrs(newSet)
      if (sockaddrs.nonEmpty) Addr.Bound(sockaddrs)
      else Addr.Neg
    }

    val stable = StabilizingAddr(
      addrOffer,
      zkHealthHandler,
      factory.sessionTimeout,
      DefaultStatsReceiver.scope("zkGroup"))

    val v = Var[Addr](Addr.Pending)
    stable foreach { newAddr =>
      v() = newAddr
    }

    v
  }

  private[this] def zkHosts(hosts: String) = {
    val zkHosts = factory.hostSet(hosts)
    if (zkHosts.isEmpty) {
      throw new ZkResolverException(
        "ZK client address \"%s\" resolves to nothing".format(hosts))
    }
    zkHosts
  }

  def bind(arg: String) = arg.split("!") match {
    // zk!host:2181!/path
    case Array(hosts, path) =>
      resolve(zkHosts(hosts), path, None)

    // zk!host:2181!/path!endpoint
    case Array(hosts, path, endpoint) =>
      resolve(zkHosts(hosts), path, Some(endpoint))

    case _ =>
      throw new ZkResolverException("Invalid address \"%s\"".format(arg))
  }
}
