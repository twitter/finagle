package com.twitter.finagle.memcached

import _root_.java.net.{SocketAddress, InetSocketAddress}
import com.google.gson.GsonBuilder
import com.twitter.common.io.{Codec,JsonCodec}
import com.twitter.common.zookeeper._
import com.twitter.finagle.{Addr, Address, Group, Resolver}
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.zookeeper.{ZkGroup, DefaultZkClientFactory}
import com.twitter.thrift.Status.ALIVE
import com.twitter.util._

object CacheNode {

  /**
   * Utility method for translating a `CacheNode` to an `Address`
   * (used when constructing a `Name` representing a `Cluster`).
   */
  private[memcached] val toAddress: CacheNode => Address = {
    case CacheNode(host, port, weight, key) =>
      val metadata = CacheNodeMetadata.toAddrMetadata(CacheNodeMetadata(weight, key))
      Address.Inet(new InetSocketAddress(host, port), metadata)
  }
}

// Type definition representing a cache node
case class CacheNode(host: String, port: Int, weight: Int, key: Option[String] = None) extends SocketAddress {
  // Use overloads to keep the same ABI
  def this(host: String, port: Int, weight: Int) = this(host, port, weight, None)
}

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

  def bind(arg: String) = {
    arg.split("!") match {
      // twcache!<host1>:<port>:<weight>:<key>,<host2>:<port>:<weight>:<key>,<host3>:<port>:<weight>:<key>
      case Array(hosts) =>
        CacheNodeGroup(hosts).set.map(toUnresolvedAddr)

      // twcache!zkhost:2181!/twitter/service/cache/<stage>/<name>
      case Array(zkHosts, path) =>
        val zkClient = DefaultZkClientFactory.get(DefaultZkClientFactory.hostSet(zkHosts))._1
        val group = CacheNodeGroup.newZkCacheNodeGroup(
          path, zkClient, ClientStatsReceiver.scope(scheme).scope(path))

        val underlyingSizeGauge = ClientStatsReceiver.scope(scheme).scope(path).addGauge("underlyingPoolSize") {
          group.members.size
        }
        group.set.map(toUnresolvedAddr)

      case _ =>
        throw new TwitterCacheResolverException(
          "Invalid twcache format \"%s\"".format(arg))
    }
  }

  private def toUnresolvedAddr(g: Set[CacheNode]): Addr = {
    val set: Set[Address] = g.map {
      case CacheNode(host, port, weight, key) =>
        val ia = InetSocketAddress.createUnresolved(host, port)
        val metadata = CacheNodeMetadata(weight, key)
        Address.Inet(ia, CacheNodeMetadata.toAddrMetadata(metadata))
    }
    Addr.Bound(set)
  }
}

// TODO: Rewrite Memcache cluster representation in terms of Var[Addr].
object CacheNodeGroup {
  // <host1>:<port>:<weight>:<key>,<host2>:<port>:<weight>:<key>,<host3>:<port>:<weight>:<key>
  def apply(hosts: String) = {
    val hostSeq = hosts.split(Array(' ', ','))
      .filter((_ != ""))
      .map(_.split(":"))
      .map {
        case Array(host)                    => (host, 11211, 1, None)
        case Array(host, port)              => (host, port.toInt, 1, None)
        case Array(host, port, weight)      => (host, port.toInt, weight.toInt, None)
        case Array(host, port, weight, key) => (host, port.toInt, weight.toInt, Some(key))
      }

    newStaticGroup(hostSeq.map {
      case (host, port, weight, key) => new CacheNode(host, port, weight, key)
    }.toSet)
  }

  def apply(group: Group[SocketAddress], useOnlyResolvedAddress: Boolean = false) = group collect {
    case node: CacheNode => node
    // Note: we ignore weights here
    case ia: InetSocketAddress if useOnlyResolvedAddress && !ia.isUnresolved =>
      //Note: unresolvedAddresses won't be added even if they are able
      // to be resolved after added
      val key = ia.getAddress.getHostAddress + ":" + ia.getPort
      new CacheNode(ia.getHostName, ia.getPort, 1, Some(key))
    case ia: InetSocketAddress if !useOnlyResolvedAddress =>
      new CacheNode(ia.getHostName, ia.getPort, 1, None)
  }

  def newStaticGroup(cacheNodeSet: Set[CacheNode]) = Group(cacheNodeSet.toSeq:_*)

  def newZkCacheNodeGroup(
    path: String, zkClient: ZooKeeperClient, statsReceiver: StatsReceiver = NullStatsReceiver
  ): Group[CacheNode] = {
    new ZkGroup(new ServerSetImpl(zkClient, path), path) collect {
      case inst if inst.getStatus == ALIVE =>
        val ep = inst.getServiceEndpoint
        val shardInfo = if (inst.isSetShard) Some(inst.getShard.toString) else None
        CacheNode(ep.getHost, ep.getPort, 1, shardInfo)
    }
  }

  private[finagle] def fromVarAddr(va: Var[Addr], useOnlyResolvedAddress: Boolean = false) = new Group[CacheNode] {
    protected[finagle] val set: Var[Set[CacheNode]] = va map {
      case Addr.Bound(addrs, _) =>
        addrs.collect {
          case Address.Inet(ia, CacheNodeMetadata(weight, key)) =>
            CacheNode(ia.getHostName, ia.getPort, weight, key)
          case Address.Inet(ia, _) if useOnlyResolvedAddress && !ia.isUnresolved =>
            val key = ia.getAddress.getHostAddress + ":" + ia.getPort
            CacheNode(ia.getHostName, ia.getPort, 1, Some(key))
          case Address.Inet(ia, _) if !useOnlyResolvedAddress=>
            CacheNode(ia.getHostName, ia.getPort, 1, None)
        }
      case _ => Set[CacheNode]()
    }
  }
}

/**
 * Cache pool config data object
 */
object CachePoolConfig {
  val jsonCodec: Codec[CachePoolConfig] =
    JsonCodec.create(classOf[CachePoolConfig],
      new GsonBuilder().setExclusionStrategies(JsonCodec.getThriftExclusionStrategy()).create())
}

/**
 * Cache pool config data format
 * Currently this data format is only used by ZookeeperCachePoolManager to read the config data
 * from zookeeper serverset parent node, and the expected cache pool size is the only attribute
 * we need for now. In the future this can be extended for other config attributes like cache
 * pool migrating state, backup cache servers list, or replication role, etc
 */
case class CachePoolConfig(cachePoolSize: Int, detectKeyRemapping: Boolean = false)
