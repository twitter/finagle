package com.twitter.finagle.memcached

import _root_.java.io.ByteArrayInputStream
import _root_.java.net.InetSocketAddress
import com.google.gson.GsonBuilder
import com.twitter.common.io.{Codec,JsonCodec}
import com.twitter.common.quantity.{Amount,Time}
import com.twitter.common.zookeeper.ZooKeeperClient
import com.twitter.common_internal.zookeeper.TwitterServerSet
import com.twitter.common_internal.zookeeper.TwitterServerSet.Service
import com.twitter.concurrent.{Broker, Spool}
import com.twitter.concurrent.Spool.*::
import com.twitter.conversions.time._
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster
import com.twitter.util._
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.{WatchedEvent, Watcher}
import scala.collection.mutable

// Type definition representing a cache node
case class CacheNode(host: String, port: Int, weight: Int)

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
   * @param cacheNodeSeq static set of cache nodes to construct the cluster
   */
  def newStaticCluster(cacheNodeSeq: Seq[CacheNode]) = new StaticCachePoolCluster(cacheNodeSeq)

  /**
   * Zookeeper based cache pool cluster.
   * The cluster will monitor the underlying serverset changes and report the detected underlying
   * pool size. The cluster snapshot will be updated during cache-team's managed operation, and
   * the Future spool will be updated with corresponding changes
   *
   * @param sdService the SD service token representing the cache pool
   * @param backupPool Optional, the backup static pool to use in case of ZK failure
   */
  def newZkCluster(sdService: Service, backupPool: Option[Set[CacheNode]]=None) =
    new ZookeeperCachePoolCluster(sdService, TwitterServerSet.createClient(sdService), backupPool)
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
 * @param cacheNodeSeq static set of cache nodes to construct the cluster
 */
class StaticCachePoolCluster(cacheNodeSeq: Seq[CacheNode]) extends CachePoolCluster {
  // TODO: stats collecting here
  private[this] var underlyingSizeGauge = cacheNodeSeq.size

  // The cache pool will updated once and only once as the underlying pool never changes
  updatePool(cacheNodeSeq.toSet)
}

/**
 * ZooKeeper based cache pool cluster companion object
 */
object ZookeeperCachePoolCluster {
  private val DefaultZkConnectionRetryBackoff =
    (Backoff.exponential(1.second, 2) take 6) ++ Backoff.const(60.seconds)
  private val CachePoolWaitCompleteTimeout = 10.seconds
  private val BackupPoolFallBackTimeout = 10.seconds
}

/**
 * SD cluster based cache pool cluster with a zookeeper serverset as the underlying pool.
 * It will monitor the underlying serverset changes and report the detected underlying pool size.
 * It will also monitor the serverset parent node for cache pool config data, cache pool cluster
 * update will be triggered whenever cache config data change event happens.
 *
 * @param sdService the SD service token representing the cache pool
 * @param zkClient zookeeper client talking to the SD cluster
 * @param backupPool Optional, the backup static pool to use in case of ZK failure
 */
class ZookeeperCachePoolCluster private[memcached](
  sdService: Service,
  zkClient: ZooKeeperClient,
  backupPool: Option[Set[CacheNode]] = None)
  extends CachePoolCluster {
  private[this] val futurePool = FuturePool.defaultPool

  private[this] val zkServerSetCluster =
    new ZookeeperServerSetCluster(TwitterServerSet.create(zkClient, sdService)) map {
      case addr: InetSocketAddress =>
        CacheNode(addr.getHostName, addr.getPort, 1)
    }

  // continuously gauging underlying cluster size
  // TODO: stats collecting here
  private[this] var underlyingSizeGauge = 0
  zkServerSetCluster.snap match {
    case (underlyingSeq, underlyingChanges) =>
      underlyingSizeGauge = underlyingSeq.size
      underlyingChanges foreach { spool =>
        spool foreach {
          case Cluster.Add(node) => underlyingSizeGauge += 1
          case Cluster.Rem(node) => underlyingSizeGauge -= 1
        }
      }
  }

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
      backoff: Stream[Duration] = ZookeeperCachePoolCluster.DefaultZkConnectionRetryBackoff
    ): Unit = {
      futurePool {
        readCachePoolConfigData(alsoUpdatePool)
      } onFailure { ex =>
        // TODO: stat here
        backoff match {
          case wait #:: rest =>
            CachePoolCluster.timer.doLater(wait) { scheduleReadCachePoolConfig(alsoUpdatePool, rest) }
        }
      } onSuccess { _ =>
      // If succeeded, loop back to consume the next work queued at the broker
        loopZookeeperWork
      }
    }

    // get one work item off the broker and schedule it into the future pool
    // if there's no work available yet, this only registers a call back to the broker so that whoever
    // provides work item would then do the scheduling
    zookeeperWorkQueue.recv.sync() onSuccess {
      case LoadCachePoolConfig => scheduleReadCachePoolConfig(alsoUpdatePool = true)
      case ReEstablishZKConn => scheduleReadCachePoolConfig(alsoUpdatePool = false)
    }
  }

  // Kick off the loop to process zookeeper work queue
  loopZookeeperWork

  private[this] val zkConnectionWatcher: Watcher = new Watcher() {
    // NOTE: Ensure that the processing of events is not a blocking operation.
    override def process(event: WatchedEvent) = {
      if (event.getType == Watcher.Event.EventType.None)
        event.getState match {
          case KeeperState.Disconnected | KeeperState.SyncConnected =>
            // TODO: stat here
          case KeeperState.Expired =>
            // TODO: stat here
            // we only need to re-establish the zk connection and re-register the config data
            // watcher when Expired event happens because this is the event that will close
            // the zk client (which will lose all attached watchers). We could also do this
            // only when SyncConnected happens instead of keep retrying, but that would be
            // assuming other components sharing the zk client (e.g. ZookeeperServerSetCluster)
            // will always actively attempts to re-establish the zk connection. For now, I'm
            // making it actively re-establishing the connection itself here.
            // TODO: signaling that the underlying zk pool management in unhealthy and let
            // the scheduled work to set it back to healthy once the work is done
            zookeeperWorkQueue ! ReEstablishZKConn
        }
    }
  }

  private[this] val cachePoolConfigDataWatcher: Watcher = new Watcher() {
    // NOTE: Ensure that the processing of events is not a blocking operation.
    override def process(event: WatchedEvent) = {
      if (event.getState == KeeperState.SyncConnected) {
        event.getType match {
          case Watcher.Event.EventType.NodeDataChanged =>
            // handle node data change event
            // TODO: stat here
            zookeeperWorkQueue ! LoadCachePoolConfig
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
  // This backup pool is mainly provided in case of long time SD cluster outage during which
  // cache client needs to be restarted.
  backupPool foreach  { pool =>
    ready within (CachePoolCluster.timer, ZookeeperCachePoolCluster.BackupPoolFallBackTimeout) onFailure {
        _ => updatePool(pool)
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
        .get(Amount.of(ZookeeperCachePoolCluster.CachePoolWaitCompleteTimeout.inMilliseconds, Time.MILLISECONDS))
        .getData(TwitterServerSet.getPath(sdService), cachePoolConfigDataWatcher, null)

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
      val newSet = waitForClusterComplete(snapshotSeq.toSet, expectedClusterSize, snapshotChanges)
          .get(ZookeeperCachePoolCluster.CachePoolWaitCompleteTimeout)()

      updatePool(newSet.toSet)
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
