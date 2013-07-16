package com.twitter.finagle.memcached

import _root_.java.io.ByteArrayInputStream
import _root_.java.net.{SocketAddress, InetSocketAddress}
import com.google.gson.GsonBuilder
import com.twitter.common.io.{Codec,JsonCodec}
import com.twitter.common.quantity.{Amount,Time}
import com.twitter.common.zookeeper.{ServerSets, ZooKeeperClient, ZooKeeperUtils}
import com.twitter.concurrent.Spool.*::
import com.twitter.concurrent.{Broker, Spool}
import com.twitter.conversions.time._
import com.twitter.finagle.{Group, Resolver}
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.zookeeper.{DefaultZkClientFactory, ZookeeperServerSetCluster}
import com.twitter.util._
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.{WatchedEvent, Watcher}
import scala.collection.mutable

// Type definition representing a cache node
case class CacheNode(host: String, port: Int, weight: Int) extends SocketAddress

/**
 * Twitter cache pool path resolver.
 */
class TwitterCacheResolverException(msg: String) extends Exception(msg)
class TwitterCacheResolver extends Resolver {
  val scheme = "twcache"

  def resolve(addr: String) = {
    addr.split("!") match {
      // twcache!host1:11211:1,host2:11211:1,host3:11211:2
      case Array(hosts) =>
        val group = Group(PartitionedClient.parseHostPortWeights(hosts) map {
          case (host, port, weight) => new CacheNode(host, port, weight): SocketAddress
        }:_*)
        Return(group)

      // twcache!zkhost:2181!/twitter/service/cache/<stage>/<name>
      case Array(zkHosts, path) =>
        val zkClient = DefaultZkClientFactory.get(DefaultZkClientFactory.hostSet(zkHosts))._1
        val cluster = new ZookeeperCachePoolCluster(
          zkPath = path,
          zkClient = zkClient,
          backupPool = None,
          statsReceiver = ClientStatsReceiver.scope(scheme).scope(path))
        Await.result(cluster.ready)
        val group = Group.fromCluster(cluster) map { case c: CacheNode => c: SocketAddress }
        Return(group)

      case _ =>
        Throw(new TwitterCacheResolverException("Invalid twcache format \"%s\"".format(addr)))
    }
  }
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
case class CachePoolConfig(cachePoolSize: Int)

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
  private val DefaultZkConnectionRetryBackoff =
    (Backoff.exponential(1.second, 2) take 6) ++ Backoff.const(60.seconds)
  private val CachePoolWaitCompleteTimeout = 10.seconds
  private val BackupPoolFallBackTimeout = 10.seconds
  private object UnderlyingZookeeperStatus extends Enumeration {
    type t = Value
    val Healthy, Disconnected, Expired = Value
  }
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
class ZookeeperCachePoolCluster private[memcached](
  zkPath: String,
  zkClient: ZooKeeperClient,
  backupPool: Option[Set[CacheNode]] = None,
  statsReceiver: StatsReceiver = NullStatsReceiver)
  extends CachePoolCluster {

  import ZookeeperCachePoolCluster._

  private[this] val futurePool = FuturePool.unboundedPool

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

  private[this] var underlyingPoolHealth = UnderlyingZookeeperStatus.Expired
  private[this] val underlyingHealthGauge = statsReceiver.addGauge("underlyingPoolHealth") { underlyingPoolHealth.id }
  private[this] val readCachePoolFailedCounter = statsReceiver.counter("readCachePoolConfig.failed")
  private[this] val readCachePoolSucceededCounter = statsReceiver.counter("readCachePoolConfig.succeeded")
  private[this] val reloadCachePoolWorkCounter = statsReceiver.counter("reloadCachePool")
  private[this] val reEstablishZkConnWorkCounter = statsReceiver.counter("reEstablishZkConnection")

  private sealed trait ZKPoolWork
  private case object LoadCachePoolConfig extends ZKPoolWork
  private case object ReEstablishZKConn extends ZKPoolWork

  private[this] val zookeeperWorkQueue = new Broker[ZKPoolWork]

  /**
   * Read work items of the broker and schedule the work with future pool. If the scheduled work
   * failed, it will repeatedly retry itself in a backoff manner (with a timer) until succeeded,
   * which will recursively call this method to restart the process again.
   *
   * This function guarantees that at any given time there will be only one thread (future pool thread)
   * blocking on zookeeper IO work. Multiple ZK connection events or cache pool change events would only
   * queue up the work, and each work will be picked up only after the previous one finished successfully
   */
  private[this] def loopZookeeperWork {
    def scheduleReadCachePoolConfig(
      alsoUpdatePool: Boolean,
      backoff: Stream[Duration] = DefaultZkConnectionRetryBackoff
    ): Unit = {
      futurePool {
        readCachePoolConfigData(alsoUpdatePool)
      } onFailure { ex =>
        readCachePoolFailedCounter.incr()
        backoff match {
          case wait #:: rest =>
            CachePoolCluster.timer.doLater(wait) { scheduleReadCachePoolConfig(alsoUpdatePool, rest) }
        }
      } onSuccess { _ =>
        // If succeeded, loop back to consume the next work queued at the broker
        readCachePoolSucceededCounter.incr()
        underlyingPoolHealth = UnderlyingZookeeperStatus.Healthy
        loopZookeeperWork
      }
    }

    // get one work item off the broker and schedule it into the future pool
    // if there's no work available yet, this only registers a call back to the broker so that whoever
    // provides work item would then do the scheduling
    zookeeperWorkQueue.recv.sync() onSuccess {
      case LoadCachePoolConfig => {
        reloadCachePoolWorkCounter.incr()
        scheduleReadCachePoolConfig(alsoUpdatePool = true)
      }
      case ReEstablishZKConn => {
        reEstablishZkConnWorkCounter.incr()
        scheduleReadCachePoolConfig(alsoUpdatePool = false)
      }
    }
  }

  // Kick off the loop to process zookeeper work queue
  loopZookeeperWork

  private[this] val zkConnectionWatcher: Watcher = new Watcher() {
    // NOTE: Ensure that the processing of events is not a blocking operation.
    override def process(event: WatchedEvent) = {
      if (event.getType == Watcher.Event.EventType.None)
        statsReceiver.counter("zkConnectionEvent." + event.getState).incr()
        event.getState match {
          // actively trying to re-establish the zk connection (hence re-register the cache pool
          // data watcher) whenever an zookeeper connection expired or disconnected. We could also
          // only do this when SyncConnected happens, but that would be assuming other components
          // sharing the zk client (e.g. ZookeeperServerSetCluster) will always actively attempts
          // to re-establish the zk connection. For now, I'm making it actively re-establishing
          // the connection itself here.
          case KeeperState.Disconnected =>
            underlyingPoolHealth = UnderlyingZookeeperStatus.Disconnected
            zookeeperWorkQueue ! ReEstablishZKConn
          case KeeperState.Expired =>
            underlyingPoolHealth = UnderlyingZookeeperStatus.Expired
            zookeeperWorkQueue ! ReEstablishZKConn
          case _ =>
        }
    }
  }

  private[this] val cachePoolConfigDataWatcher: Watcher = new Watcher() {
    // NOTE: Ensure that the processing of events is not a blocking operation.
    override def process(event: WatchedEvent) = {
      if (event.getState == KeeperState.SyncConnected) {
        statsReceiver.counter("cachePoolChangeEvent." + event.getType).incr()
        event.getType match {
          case Watcher.Event.EventType.NodeDataChanged =>
            // handle node data change event
            zookeeperWorkQueue ! LoadCachePoolConfig
          case _ =>
        }
      }
    }
  }

  // Register top-level connection watcher to monitor zk change.
  // This watcher will live across different zk connection
  zkClient.register(zkConnectionWatcher)

  // Read the config data and update the pool for the first time during constructing
  // This will also attach a data change event watcher (cachePoolConfigDataWatcher) to
  // the zk client so that future config data change event will trigger this again.
  zookeeperWorkQueue ! LoadCachePoolConfig

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

  /**
   * Read the immediate parent zookeeper node data for the cache pool config data, then invoke
   * the cache pool update function once the configured requirement is met, as well as attaching
   * a config data watcher which will perform the same action automatically whenever the config
   * data is changed again
   */
  private[this] def readCachePoolConfigData(alsoUpdatePool: Boolean): Unit = synchronized {
    // read cache pool config data and attach the node data change watcher
    val data = zkClient
        .get(Amount.of(CachePoolWaitCompleteTimeout.inMilliseconds, Time.MILLISECONDS))
        .getData(zkPath, cachePoolConfigDataWatcher, null)

    if (alsoUpdatePool && data != null) {
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
