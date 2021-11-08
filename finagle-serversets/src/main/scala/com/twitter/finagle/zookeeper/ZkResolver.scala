package com.twitter.finagle.zookeeper

import com.twitter.concurrent.Broker
import com.twitter.concurrent.Offer
import com.twitter.finagle.addr.StabilizingAddr
import com.twitter.finagle.common.net.pool.DynamicHostSet
import com.twitter.finagle.common.net.pool.DynamicHostSet.MonitorException
import com.twitter.finagle.common.zookeeper.ServerSet
import com.twitter.finagle.common.zookeeper.ServerSetImpl
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.Addr
import com.twitter.finagle.Address
import com.twitter.finagle.Resolver
import com.twitter.thrift.Endpoint
import com.twitter.thrift.ServiceInstance
import com.twitter.util.Var
import java.net.InetSocketAddress
import java.util.logging.Level
import java.util.logging.Logger
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Indicates that a failure occurred while attempting to resolve a cluster
 * using a [[com.twitter.finagle.zookeeper.ZkAnnouncer]].
 */
@deprecated("Prefer com.twitter.finagle.serverset2.Zk2Resolver", "2019-02-13")
class ZkResolverException(msg: String) extends Exception(msg)

private class ZkOffer(serverSet: ServerSet, path: String)
    extends Thread("ZkOffer(%s)".format(path))
    with Offer[Set[ServiceInstance]] {
  setDaemon(true)
  start()

  private[this] val inbound = new Broker[Set[ServiceInstance]]
  private[this] val log = Logger.getLogger("zkoffer")

  override def run(): Unit = {
    try {
      serverSet.watch(new DynamicHostSet.HostChangeMonitor[ServiceInstance] {
        def onChange(newSet: java.util.Set[ServiceInstance]): Unit = {
          inbound !! newSet.asScala.toSet
        }
      })
    } catch {
      case exc: MonitorException =>
        // There are certain path permission checks in the serverset library
        // that can cause exceptions here. We'll send an empty set (which
        // becomes a negative resolution).
        log.log(
          Level.WARNING,
          "Exception when trying to watch a ServerSet! " +
            "Returning negative resolution.",
          exc
        )
        inbound !! Set.empty
    }
  }

  def prepare() = inbound.recv.prepare()
}

@deprecated("Prefer com.twitter.finagle.serverset2.Zk2Resolver", "2019-02-13")
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
    shardId: Option[Int] = None
  ): Var[Addr] =
    synchronized {
      cache.getOrElseUpdate(
        (zkHosts, path, endpoint, shardId),
        newVar(zkHosts, path, newToAddresses(endpoint, shardId))
      )
    }

  private def newToAddresses(endpoint: Option[String], shardId: Option[Int]) = {

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

    val toAddress: Endpoint => Address = (ep: Endpoint) => Address(ep.getHost, ep.getPort)

    (insts: Set[ServiceInstance]) =>
      insts.collect(filterShardId).collect(getEndpoint).map(toAddress)
  }

  private def newVar(
    zkHosts: Set[InetSocketAddress],
    path: String,
    toAddresses: Set[ServiceInstance] => Set[Address]
  ) = {

    val (zkClient, zkHealthHandler) = factory.get(zkHosts)
    val zkOffer = new ZkOffer(new ServerSetImpl(zkClient, path), path)
    val addrOffer = zkOffer map { newSet =>
      val sockaddrs = toAddresses(newSet)
      if (sockaddrs.nonEmpty) Addr.Bound(sockaddrs)
      else Addr.Neg
    }

    val stable = StabilizingAddr(
      addrOffer,
      zkHealthHandler,
      factory.sessionTimeout,
      DefaultStatsReceiver.scope("zkGroup")
    )

    val v = Var[Addr](Addr.Pending)
    stable foreach { newAddr => v() = newAddr }

    v
  }

  private[this] def zkHosts(hosts: String) = {
    val zkHosts = factory.hostSet(hosts)
    if (zkHosts.isEmpty) {
      throw new ZkResolverException("ZK client address \"%s\" resolves to nothing".format(hosts))
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
