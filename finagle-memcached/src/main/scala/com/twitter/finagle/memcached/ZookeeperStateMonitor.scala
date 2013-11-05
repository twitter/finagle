package com.twitter.finagle.memcached

import com.twitter.common.quantity.{Time, Amount}
import com.twitter.common.zookeeper.ZooKeeperClient
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Duration, JavaTimer, FuturePool}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.{WatchedEvent, Watcher}
import scala.collection.JavaConversions._


object ZookeeperStateMonitor {
  val DefaultZkConnectionRetryBackoff =
    (Backoff.exponential(1.second, 2) take 6) ++ Backoff.const(60.seconds)
  val DefaultFuturePool = FuturePool.unboundedPool
  val DefaultTimer = new JavaTimer(isDaemon = true)
  val DefaultZKWaitTimeout = 10.seconds
}

/**
 * A zk monitor trait that assists with monitoring a given zk path for any node data change,
 * in which the provided zk data handling implementation will be invoked.
 *
 * This monitor will maintain a queue so that every work item triggered by zk event will be
 * processed in an order with a back off policy. It also set-up a zookeeper connection watcher
 * by default to re-set the data change watcher even during zk re-connect.
 *
 * The monitor will set-up all watcher properly kick off the loop to process future event;
 * you can also invoke loadZKData() in your class anytime to force reading zk data and apply it.
 *
 * Example use cases are:
 * - zookeeper based CachePoolCluster uses this to monitor cache pool members change
 * - zookeeper based MigrationClient uses this ot monitor migration state transitioning
 */
trait ZookeeperStateMonitor {
  protected val zkPath: String
  protected val zkClient: ZooKeeperClient
  protected val statsReceiver: StatsReceiver

  import ZookeeperStateMonitor._

  private[this] val zkWorkFailedCounter = statsReceiver.counter("zkWork.failed")
  private[this] val zkWorkSucceededCounter = statsReceiver.counter("zkWork.succeeded")
  private[this] val loadZKDataCounter = statsReceiver.counter("loadZKData")
  private[this] val loadZKChildrenCounter = statsReceiver.counter("loadZKChildren")
  private[this] val reconnectZKCounter = statsReceiver.counter("reconnectZK")

  private[this] val zookeeperWorkQueue = new Broker[() => Unit]

  // zk watcher for connection, data, children event
  private[this] val zkWatcher: Watcher = new Watcher() {
    // NOTE: Ensure that the processing of events is not a blocking operation.
    override def process(event: WatchedEvent) = {
      event.getState match {
        // actively trying to re-establish the zk connection (hence re-register the zk path
        // data watcher) whenever an zookeeper connection expired or disconnected. We could also
        // only do this when SyncConnected happens, but that would be assuming other components
        // sharing the zk client (e.g. ZookeeperServerSetCluster) will always actively attempts
        // to re-establish the zk connection. For now, I'm making it actively re-establishing
        // the connection itself here.
        case (KeeperState.Disconnected | KeeperState.Expired) if event.getType == Watcher.Event.EventType.None =>
          statsReceiver.counter("zkConnectionEvent." + event.getState).incr()
          zookeeperWorkQueue ! reconnectZK
        case KeeperState.SyncConnected =>
          statsReceiver.counter("zkNodeChangeEvent." + event.getType).incr()
          event.getType match {
            case Watcher.Event.EventType.NodeDataChanged =>
              zookeeperWorkQueue ! loadZKData
            case Watcher.Event.EventType.NodeChildrenChanged =>
              zookeeperWorkQueue ! loadZKChildren
            case _ =>
          }
        case _ =>
      }
    }
  }

  /**
   * Read work items of the broker and schedule the work with future pool. If the scheduled work
   * failed, it will repeatedly retry itself in a backoff manner (with a timer) until succeeded,
   * which then recursively call this method to process the next work item.
   *
   * This function guarantees that at any given time there will be only one thread (future pool thread)
   * blocking on zookeeper IO work. Multiple ZK connection events or cache pool change events would only
   * queue up the work, and each work will be picked up only after the previous one finished successfully
   */
  private[this] def loopZookeeperWork {
    def scheduleReadCachePoolConfig(
        op: () => Unit,
        backoff: Stream[Duration] = DefaultZkConnectionRetryBackoff
        ): Unit = {
      DefaultFuturePool {
        op()
      } onFailure { ex =>
        zkWorkFailedCounter.incr()
        backoff match {
          case wait #:: rest =>
            DefaultTimer.doLater(wait) { scheduleReadCachePoolConfig(op, rest) }
        }
      } onSuccess { _ =>
        zkWorkSucceededCounter.incr()
        loopZookeeperWork
      }
    }

    // get one work item off the broker and schedule it into the future pool
    zookeeperWorkQueue.recv.sync() onSuccess {
      case op: (() => Unit) => {
        scheduleReadCachePoolConfig(op)
      }
    }
  }

  /**
   * Load the zookeeper node data as well as leaving a data watch, then invoke the
   * applyZKData implementation to process the data string.
   */
  def applyZKData(data: Array[Byte]): Unit
  def loadZKData = () => synchronized {
    loadZKDataCounter.incr()

    // read cache pool config data and leave a node data watch
    val data = zkClient
        .get(Amount.of(DefaultZKWaitTimeout.inMilliseconds, Time.MILLISECONDS))
        .getData(zkPath, true, null)

    applyZKData(data)
  }

  /**
   * Load the zookeeper node children as well as leaving a children watch, then invoke the
   * applyZKChildren implementation to process the children list.
   */
  def applyZKChildren(children: List[String]): Unit = {} // no-op by default
  def loadZKChildren = () => synchronized {
    loadZKChildrenCounter.incr()

    // get children list and leave a node children watch
    val children = zkClient
        .get(Amount.of(DefaultZKWaitTimeout.inMilliseconds, Time.MILLISECONDS))
        .getChildren(zkPath, true, null)

    applyZKChildren(children.toList)
  }

  /**
   * Reconnect to the zookeeper, this maybe invoked when zookeeper connection expired and the
   * node data watcher previously registered got dropped, hence re-attache the data wather here.
   */
  def reconnectZK = () => synchronized {
    reconnectZKCounter.incr()

    // reset watch for node data and children
    val data = zkClient
        .get(Amount.of(DefaultZKWaitTimeout.inMilliseconds, Time.MILLISECONDS))
        .getData(zkPath, true, null)
    val children = zkClient
        .get(Amount.of(DefaultZKWaitTimeout.inMilliseconds, Time.MILLISECONDS))
        .getChildren(zkPath, true, null)
  }

  // Register top-level connection watcher to monitor zk change.
  // This watcher will live across different zk connection
  zkClient.register(zkWatcher)

  // attach data/children change event watcher to the zk client for the first time during constructing
  zookeeperWorkQueue ! loadZKData
  zookeeperWorkQueue ! loadZKChildren

  // Kick off the loop to process zookeeper work queue
  loopZookeeperWork
}
