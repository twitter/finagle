package com.twitter.finagle.cacheresolver

import _root_.java.io.ByteArrayInputStream
import _root_.java.net.{SocketAddress, InetSocketAddress}
import com.google.gson.GsonBuilder
import com.twitter.common.io.{Codec,JsonCodec}
import com.twitter.common.zookeeper._
import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.*::
import com.twitter.conversions.time._
import com.twitter.finagle.{Group, Resolver, Addr, WeightedInetSocketAddress}
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.zookeeper.{ZkGroup, DefaultZkClientFactory, ZookeeperServerSetCluster}
import com.twitter.finagle.{Group, Resolver}
import com.twitter.thrift.ServiceInstance
import com.twitter.thrift.Status.ALIVE
import com.twitter.util._
import scala.collection.mutable

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
        val group = CacheNodeGroup(hosts) map {
          case node: CacheNode => node: SocketAddress
        }
        group.set map { newSet => Addr.Bound(newSet) }

      // twcache!zkhost:2181!/twitter/service/cache/<stage>/<name>
      case Array(zkHosts, path) =>
        val zkClient = DefaultZkClientFactory.get(DefaultZkClientFactory.hostSet(zkHosts))._1
        val group = CacheNodeGroup.newZkCacheNodeGroup(
          path, zkClient, ClientStatsReceiver.scope(scheme).scope(path)
        ) map { case c: CacheNode => c: SocketAddress }
        group.set map { newSet => Addr.Bound(newSet) }

      case _ =>
        throw new TwitterCacheResolverException(
          "Invalid twcache format \"%s\"".format(arg))
    }
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

    // TODO: should we use the weights propagated here? The weights passed
    // by WeightedInetSocketAddress are doubles -- should we discretize these?
    case WeightedInetSocketAddress(ia, weight)
    if useOnlyResolvedAddress && !ia.isUnresolved =>
      //Note: unresolvedAddresses won't be added even if they are able
      // to be resolved after added
      new CacheNode(ia.getHostName, ia.getPort, 1,
        Some(ia.getAddress.getHostAddress + ":" + ia.getPort))
    case WeightedInetSocketAddress(ia, weight) if !useOnlyResolvedAddress =>
      new CacheNode(ia.getHostName, ia.getPort, 1, None)
  }

  def newStaticGroup(cacheNodeSet: Set[CacheNode]) = Group(cacheNodeSet.toSeq:_*)

  def newZkCacheNodeGroup(
    path: String, zkClient: ZooKeeperClient, statsReceiver: StatsReceiver = NullStatsReceiver
  ) = new ZookeeperCacheNodeGroup(zkPath = path, zkClient = zkClient, statsReceiver = statsReceiver)
}

/**
 * Cache specific cluster implementation.
 * - A cache pool is a Cluster of cache nodes.
 * - cache pool requires a underlying pool manager as the source of the cache nodes
 * - the underlying pool manager encapsulates logic of monitoring the cache node changes and
 * deciding when to update the cache pool cluster
 */
object CachePoolCluster {
  val timer = new JavaTimer(isDaemon = true)

  /**
   *  Cache pool based on a static list
   * @param cacheNodeSet static set of cache nodes to construct the cluster
   */
  def newStaticCluster(cacheNodeSet: Set[CacheNode]) = new StaticCachePoolCluster(cacheNodeSet)

  /**
   * Zookeeper based cache pool cluster.
   * The cluster will monitor the underlying serverset changes and report the detected underlying
   * pool size. The cluster snapshot will be updated during cache-team's managed operation, and
   * the Future spool will be updated with corresponding changes
   *
   * @param zkPath the zookeeper path representing the cache pool
   * @param zkClient zookeeper client talking to the zookeeper, it will only be used to read zookeeper
   * @param backupPool Optional, the backup static pool to use in case of ZK failure. Empty pool means
   *                   the same as no backup pool.
   * @param statsReceiver Optional, the destination to report the stats to
   */
  def newZkCluster(zkPath: String, zkClient: ZooKeeperClient, backupPool: Option[Set[CacheNode]] = None, statsReceiver: StatsReceiver = NullStatsReceiver) =
    new ZookeeperCachePoolCluster(zkPath, zkClient, backupPool, statsReceiver)

  /**
   * Zookeeper based cache pool cluster.
   * The cluster will monitor the underlying serverset changes and report the detected underlying
   * pool size. The cluster snapshot is unmanaged in a way that any serverset change will be immediately
   * reflected.
   *
   * @param zkPath the zookeeper path representing the cache pool
   * @param zkClient zookeeper client talking to the zookeeper, it will only be used to read zookeeper
   */
  def newUnmanagedZkCluster(
    zkPath: String,
    zkClient: ZooKeeperClient
  ) = new ZookeeperServerSetCluster(
    ServerSets.create(zkClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath)
  ) map { case addr: InetSocketAddress =>
    CacheNode(addr.getHostName, addr.getPort, 1)
  }
}

trait CachePoolCluster extends Cluster[CacheNode] {
  /**
   * Cache pool snapshot and future changes
   * These two should only change when a key-ring rehashing is needed (e.g. cache pool
   * initialization, migration, expansion, etc), thus we only let the underlying pool manager
   * to change them
   */
  private[this] val cachePool = new mutable.HashSet[CacheNode]
  private[this] var cachePoolChanges = new Promise[Spool[Cluster.Change[CacheNode]]]

  def snap: (Seq[CacheNode], Future[Spool[Cluster.Change[CacheNode]]]) = cachePool synchronized {
    (cachePool.toSeq, cachePoolChanges)
  }

  /**
   * TODO: pick up new rev of Cluster once it's ready
   * Soon enough the Cluster will be defined in a way that we can directly managing the managers
   * in a more flexible way, by then we should be able to do batch update we want here. For now,
   * the updating pool is still done one by one.
   */
  final protected[this] def updatePool(newSet: Set[CacheNode]) = cachePool synchronized  {
    val added = newSet &~ cachePool
    val removed = cachePool &~ newSet

    // modify cachePool and cachePoolChanges
    removed foreach { node =>
      cachePool -= node
      appendUpdate(Cluster.Rem(node))
    }
    added foreach { node =>
      cachePool += node
      appendUpdate(Cluster.Add(node))
    }
  }

  private[this] def appendUpdate(update: Cluster.Change[CacheNode]) = cachePool synchronized  {
    val newTail = new Promise[Spool[Cluster.Change[CacheNode]]]
    cachePoolChanges() = Return(update *:: newTail)
    cachePoolChanges = newTail
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

/**
 *  Cache pool based on a static list
 * @param cacheNodeSet static set of cache nodes to construct the cluster
 */
class StaticCachePoolCluster(cacheNodeSet: Set[CacheNode]) extends CachePoolCluster {
  // The cache pool will updated once and only once as the underlying pool never changes
  updatePool(cacheNodeSet)
}

/**
 * ZooKeeper based cache pool cluster companion object
 */
object ZookeeperCachePoolCluster {
  private val CachePoolWaitCompleteTimeout = 10.seconds
  private val BackupPoolFallBackTimeout = 10.seconds
}

/**
 * Zookeeper based cache pool cluster with a serverset as the underlying pool.
 * It will monitor the underlying serverset changes and report the detected underlying pool size.
 * It will also monitor the serverset parent node for cache pool config data, cache pool cluster
 * update will be triggered whenever cache config data change event happens.
 *
 * @param zkPath the zookeeper path representing the cache pool
 * @param zkClient zookeeper client talking to the zookeeper, it will only be used to read zookeeper
 * @param backupPool Optional, the backup static pool to use in case of ZK failure. Empty pool means
 *                   the same as no backup pool.
 * @param statsReceiver Optional, the destination to report the stats to
 */
class ZookeeperCachePoolCluster private[cacheresolver](
  protected val zkPath: String,
  protected val zkClient: ZooKeeperClient,
  backupPool: Option[Set[CacheNode]] = None,
  protected val statsReceiver: StatsReceiver = NullStatsReceiver)
  extends CachePoolCluster with ZookeeperStateMonitor {

  import ZookeeperCachePoolCluster._

  private[this] val zkServerSetCluster =
    new ZookeeperServerSetCluster(
      ServerSets.create(zkClient, ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL, zkPath)) map {
      case addr: InetSocketAddress =>
        CacheNode(addr.getHostName, addr.getPort, 1)
    }

  @volatile private[this] var underlyingSize = 0
  zkServerSetCluster.snap match {
    case (current, changes) =>
      underlyingSize = current.size
      changes foreach { spool =>
        spool foreach {
          case Cluster.Add(node) => underlyingSize += 1
          case Cluster.Rem(node) => underlyingSize -= 1
        }
      }
  }

  // continuously gauging underlying cluster size
  private[this] val underlyingSizeGauge = statsReceiver.addGauge("underlyingPoolSize") {
    underlyingSize
  }

  // Falling back to use the backup pool (if provided) after a certain timeout.
  // Meanwhile, the first time invoke of updating pool will still proceed once it successfully
  // get the underlying pool config data and a complete pool members ready, by then it
  // will overwrite the backup pool.
  // This backup pool is mainly provided in case of long time zookeeper outage during which
  // cache client needs to be restarted.
  backupPool foreach  { pool =>
      if (!pool.isEmpty) {
        ready within (CachePoolCluster.timer, BackupPoolFallBackTimeout) onFailure {
            _ => updatePool(pool)
      }
    }
  }

  override def applyZKData(data: Array[Byte]): Unit = {
    if(data != null) {
      val cachePoolConfig = CachePoolConfig.jsonCodec.deserialize(new ByteArrayInputStream(data))

      // apply the cache pool config to the cluster
      val expectedClusterSize = cachePoolConfig.cachePoolSize
      val (snapshotSeq, snapshotChanges) = zkServerSetCluster.snap

      // TODO: this can be blocking or non-blocking, depending on the protocol
      // for now I'm making it blocking call as the current known scenario is that cache config data
      // should be always exactly matching existing memberships, controlled by cache-team operator.
      // It will only block for 10 seconds after which it should trigger alerting metrics and schedule
      // another try
      val newSet = Await.result(waitForClusterComplete(snapshotSeq.toSet, expectedClusterSize, snapshotChanges),
        CachePoolWaitCompleteTimeout)

      updatePool(newSet)
    }
  }

  /**
   * Wait for the current set to contain expected size of members.
   * If the underlying zk cluster change is triggered by operator (for migration/expansion etc), the
   * config data change should always happen after the operator has verified that this zk pool manager
   * already see expected size of members, in which case this method would immediately return;
   * however during the first time this pool manager is initialized, it's possible that the zkServerSetCluster
   * hasn't caught up all existing members yet hence this method may need to wait for the future changes.
   */
  private[this] def waitForClusterComplete(
    currentSet: Set[CacheNode],
    expectedSize: Int,
    spoolChanges: Future[Spool[Cluster.Change[CacheNode]]]
  ): Future[Set[CacheNode]] = {
    if (expectedSize == currentSet.size) {
      Future.value(currentSet)
    } else spoolChanges flatMap { spool =>
      spool match {
        case Cluster.Add(node) *:: tail =>
          waitForClusterComplete(currentSet + node, expectedSize, tail)
        case Cluster.Rem(node) *:: tail =>
          // this should not happen in general as this code generally is only for first time pool
          // manager initialization
          waitForClusterComplete(currentSet - node, expectedSize, tail)
      }
    }
  }
}

/**
 * Zookeeper based cache node group with a serverset as the underlying pool.
 * It will monitor the underlying serverset changes and report the detected underlying pool size.
 * It will monitor the serverset parent node for cache pool config data, cache node group
 * update will be triggered whenever cache config data change event happens.
 *
 * @param zkPath the zookeeper path representing the cache pool
 * @param zkClient zookeeper client talking to the zookeeper, it will only be used to read zookeeper
 * @param statsReceiver Optional, the destination to report the stats to
 */
class ZookeeperCacheNodeGroup(
  protected val zkPath: String,
  protected val zkClient: ZooKeeperClient,
  protected val statsReceiver: StatsReceiver = NullStatsReceiver
) extends Group[CacheNode] with ZookeeperStateMonitor {

  protected[finagle] val set = Var(Set[CacheNode]())

  @volatile private var detectKeyRemapping = false

  private val zkGroup =
    new ZkGroup(new ServerSetImpl(zkClient, zkPath), zkPath) collect {
      case inst if inst.getStatus == ALIVE =>
        val ep = inst.getServiceEndpoint
        val shardInfo = if (inst.isSetShard) Some(inst.getShard.toString) else None
        CacheNode(ep.getHost, ep.getPort, 1, shardInfo)
    }

  private[this] val underlyingSizeGauge = statsReceiver.addGauge("underlyingPoolSize") {
    zkGroup.members.size
  }

  def applyZKData(data: Array[Byte]) {
    if(data != null) {
      val cachePoolConfig = CachePoolConfig.jsonCodec.deserialize(new ByteArrayInputStream(data))

      detectKeyRemapping = cachePoolConfig.detectKeyRemapping

      // apply the cache pool config to the cluster
      val expectedGroupSize = cachePoolConfig.cachePoolSize
      if (expectedGroupSize != zkGroup.members.size)
        throw new IllegalStateException("Underlying group size not equal to expected size")

      set() = zkGroup.members
    }
  }

  // when enabled, monitor and apply new members in case of pure cache node key remapping
  override def applyZKChildren(children: List[String]) = if (detectKeyRemapping) {
    val newMembers = zkGroup.members
    if (newMembers.size != children.size)
      throw new IllegalStateException("Underlying children size not equal to expected children size")

    if (newMembers.size == members.size) {
      val removed = (members &~ newMembers)
      val added = (newMembers &~ members)

      // pick up the diff only if new members contains exactly the same set of cache node keys,
      // e.g. certain cache node key is re-assigned to another host
      if (removed.forall(_.key.isDefined) && added.forall(_.key.isDefined) &&
          removed.size == added.size && removed.map(_.key.get) == added.map(_.key.get)) {
        set() = newMembers
      }
    }
  }
}
