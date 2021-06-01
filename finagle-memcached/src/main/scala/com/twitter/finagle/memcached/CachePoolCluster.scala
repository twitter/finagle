package com.twitter.finagle.memcached

import _root_.java.net.{InetSocketAddress, SocketAddress}
import com.twitter.finagle.{Addr, Address, Group, Resolver}
import com.twitter.finagle.common.zookeeper._
import com.twitter.finagle.partitioning.{PartitionNode, PartitionNodeMetadata}
import com.twitter.finagle.zookeeper.{DefaultZkClientFactory, ZkGroup}
import com.twitter.thrift.Status.ALIVE
import com.twitter.util._

/**
 * Indicates that an error occurred while resolving a cache address.
 * See [[com.twitter.finagle.memcached.TwitterCacheResolver]] for details.
 */
class TwitterCacheResolverException(msg: String) extends Exception(msg)

/**
 * A [[com.twitter.finagle.Resolver]] for resolving destination names associated
 * with Twitter cache pools.
 */
class TwitterCacheResolver extends Resolver {
  val scheme = "twcache"

  def bind(arg: String): Var[Addr] = {
    arg.split("!") match {
      // twcache!<host1>:<port>:<weight>:<key>,<host2>:<port>:<weight>:<key>,<host3>:<port>:<weight>:<key>
      case Array(hosts) =>
        CacheNodeGroup(hosts).set.map(toUnresolvedAddr)

      // twcache!zkhost:2181!/twitter/service/cache/<stage>/<name>
      case Array(zkHosts, path) =>
        val zkClient = DefaultZkClientFactory.get(DefaultZkClientFactory.hostSet(zkHosts))._1
        val group = CacheNodeGroup.newZkCacheNodeGroup(
          path,
          zkClient
        )
        group.set.map(toUnresolvedAddr)

      case _ =>
        throw new TwitterCacheResolverException("Invalid twcache format \"%s\"".format(arg))
    }
  }

  private def toUnresolvedAddr(g: Set[PartitionNode]): Addr = {
    val set: Set[Address] = g.map {
      case PartitionNode(host, port, weight, key) =>
        val ia = InetSocketAddress.createUnresolved(host, port)
        val metadata = PartitionNodeMetadata(weight, key)
        Address.Inet(ia, PartitionNodeMetadata.toAddrMetadata(metadata))
    }
    Addr.Bound(set)
  }
}

// TODO: Rewrite Memcache cluster representation in terms of Var[Addr].
object CacheNodeGroup {
  // <host1>:<port>:<weight>:<key>,<host2>:<port>:<weight>:<key>,<host3>:<port>:<weight>:<key>
  def apply(hosts: String): Group[PartitionNode] = {
    val hostSeq = hosts
      .split(Array(' ', ','))
      .filter((_ != ""))
      .map(_.split(":"))
      .map {
        case Array(host) => (host, 11211, 1, None)
        case Array(host, port) => (host, port.toInt, 1, None)
        case Array(host, port, weight) => (host, port.toInt, weight.toInt, None)
        case Array(host, port, weight, key) => (host, port.toInt, weight.toInt, Some(key))
      }

    newStaticGroup(hostSeq.map {
      case (host, port, weight, key) => new PartitionNode(host, port, weight, key)
    }.toSet)
  }

  def apply(
    group: Group[SocketAddress],
    useOnlyResolvedAddress: Boolean = false
  ): Group[PartitionNode] = group collect {
    case node: PartitionNode => node
    // Note: we ignore weights here
    case ia: InetSocketAddress if useOnlyResolvedAddress && !ia.isUnresolved =>
      //Note: unresolvedAddresses won't be added even if they are able
      // to be resolved after added
      val key = ia.getAddress.getHostAddress + ":" + ia.getPort
      new PartitionNode(ia.getHostName, ia.getPort, 1, Some(key))
    case ia: InetSocketAddress if !useOnlyResolvedAddress =>
      new PartitionNode(ia.getHostName, ia.getPort, 1, None)
  }

  def newStaticGroup(cacheNodeSet: Set[PartitionNode]): Group[PartitionNode] = Group(
    cacheNodeSet.toSeq: _*)

  def newZkCacheNodeGroup(
    path: String,
    zkClient: ZooKeeperClient
  ): Group[PartitionNode] = {
    new ZkGroup(new ServerSetImpl(zkClient, path), path) collect {
      case inst if inst.getStatus == ALIVE =>
        val ep = inst.getServiceEndpoint
        val shardInfo = if (inst.isSetShard) Some(inst.getShard.toString) else None
        PartitionNode(ep.getHost, ep.getPort, 1, shardInfo)
    }
  }

  private[finagle] def fromVarAddr(
    va: Var[Addr],
    useOnlyResolvedAddress: Boolean = false
  ): Group[PartitionNode] =
    new Group[PartitionNode] {
      protected[finagle] val set: Var[Set[PartitionNode]] = va map {
        case Addr.Bound(addrs, _) =>
          addrs.collect {
            case Address.Inet(ia, PartitionNodeMetadata(weight, key)) =>
              PartitionNode(ia.getHostName, ia.getPort, weight, key)
            case Address.Inet(ia, _) if useOnlyResolvedAddress && !ia.isUnresolved =>
              val key = ia.getAddress.getHostAddress + ":" + ia.getPort
              PartitionNode(ia.getHostName, ia.getPort, 1, Some(key))
            case Address.Inet(ia, _) if !useOnlyResolvedAddress =>
              PartitionNode(ia.getHostName, ia.getPort, 1, None)
          }
        case _ => Set[PartitionNode]()
      }
    }
}
