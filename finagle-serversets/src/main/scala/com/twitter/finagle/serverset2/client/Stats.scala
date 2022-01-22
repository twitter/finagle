package com.twitter.finagle.serverset2.client

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.Stat
import com.twitter.io.Buf
import com.twitter.util._

private[serverset2] trait StatsClient extends ZooKeeperClient {
  sealed trait StatFilter {
    val name: String
    lazy val failure = stats.counter(s"${name}_failures")
    lazy val success = stats.counter(s"${name}_successes")

    def apply[T](result: Future[T]): Future[T] = {
      Stat
        .timeFuture(stats.stat(s"${name}_latency_ms"))(result)
        .respond {
          case Return(_) =>
            success.incr()
          case Throw(ke: KeeperException) =>
            stats.counter(ke.name).incr()
          case Throw(_) =>
            failure.incr()
        }
      result
    }
  }

  protected val EphemeralFilter = new StatFilter {
    val name = "ephemeral"
  }

  protected val MultiFilter = new StatFilter {
    val name = "multi"
  }

  protected val ReadFilter = new StatFilter {
    val name = "read"
  }

  protected val WatchFilter = new StatFilter {
    val name = "watch"
  }

  protected val WriteFilter = new StatFilter {
    val name = "write"
  }

  protected val underlying: ZooKeeperClient
  protected val stats: StatsReceiver

  def addAuthInfo(scheme: String, auth: Buf): Future[Unit] = underlying.addAuthInfo(scheme, auth)
  def close(deadline: Time): Future[Unit] = underlying.close()
  def getEphemerals(): Future[Seq[String]] = EphemeralFilter(underlying.getEphemerals())
  def sessionId: Long = underlying.sessionId
  def sessionPasswd: Buf = underlying.sessionPasswd
  def sessionTimeout: Duration = underlying.sessionTimeout
}

private[serverset2] trait StatsReader extends StatsClient with ZooKeeperReader {
  protected val underlying: ZooKeeperReader

  def exists(path: String): Future[Option[Data.Stat]] = ReadFilter(underlying.exists(path))
  def existsWatch(path: String): Future[Watched[Option[Data.Stat]]] =
    WatchFilter(underlying.existsWatch(path))
  def getData(path: String): Future[Node.Data] = ReadFilter(underlying.getData(path))
  def getDataWatch(path: String): Future[Watched[Node.Data]] =
    WatchFilter(underlying.getDataWatch(path))
  def getACL(path: String): Future[Node.ACL] = ReadFilter(underlying.getACL(path))
  def getChildren(path: String): Future[Node.Children] = ReadFilter(underlying.getChildren(path))
  def getChildrenWatch(path: String): Future[Watched[Node.Children]] =
    WatchFilter(underlying.getChildrenWatch(path))

  def sync(path: String): Future[Unit] = ReadFilter(underlying.sync(path))
}

private[serverset2] trait StatsWriter extends StatsClient with ZooKeeperWriter {
  protected val underlying: ZooKeeperWriter

  def create(
    path: String,
    data: Option[Buf],
    acl: Seq[Data.ACL],
    createMode: CreateMode
  ): Future[String] = createMode match {
    case CreateMode.Ephemeral => EphemeralFilter(underlying.create(path, data, acl, createMode))
    case CreateMode.EphemeralSequential =>
      EphemeralFilter(underlying.create(path, data, acl, createMode))
    case _ => WriteFilter(underlying.create(path, data, acl, createMode))
  }

  def delete(path: String, version: Option[Int]): Future[Unit] =
    WriteFilter(underlying.delete(path, version))

  def setACL(path: String, acl: Seq[Data.ACL], version: Option[Int]): Future[Data.Stat] =
    WriteFilter(underlying.setACL(path, acl, version))

  def setData(path: String, data: Option[Buf], version: Option[Int]): Future[Data.Stat] =
    WriteFilter(underlying.setData(path, data, version))
}

private[serverset2] trait StatsMulti extends StatsClient with ZooKeeperMulti {
  protected val underlying: ZooKeeperMulti

  def multi(ops: Seq[Op]): Future[Seq[OpResult]] = MultiFilter(underlying.multi(ops))
}

private[serverset2] trait StatsRW extends ZooKeeperRW with StatsReader with StatsWriter {
  protected val underlying: ZooKeeperRW
}

private[serverset2] trait StatsRWMulti
    extends ZooKeeperRWMulti
    with StatsReader
    with StatsWriter
    with StatsMulti {
  protected val underlying: ZooKeeperRWMulti
}

private[serverset2] trait EventStats {
  import NodeEvent._

  protected val stats: StatsReceiver

  private[this] lazy val createdCounter = stats.counter(Created.name)
  private[this] lazy val dataChangedCounter = stats.counter(DataChanged.name)
  private[this] lazy val deletedCounter = stats.counter(Deleted.name)
  private[this] lazy val childrenChangedCounter = stats.counter(ChildrenChanged.name)
  private[this] lazy val dataWatchRemovedCounter = stats.counter(DataWatchRemoved.name)
  private[this] lazy val childWatchRemovedCounter = stats.counter(ChildWatchRemoved.name)

  protected def EventFilter(event: NodeEvent): NodeEvent = {
    event match {
      case Created => createdCounter.incr()
      case DataChanged => dataChangedCounter.incr()
      case Deleted => deletedCounter.incr()
      case ChildrenChanged => childrenChangedCounter.incr()
      case DataWatchRemoved => dataWatchRemovedCounter.incr()
      case ChildWatchRemoved => childWatchRemovedCounter.incr()
    }
    event
  }
}

object SessionStats {
  def watcher(
    underlying: Var[WatchState],
    statsReceiver: StatsReceiver,
    interval: Duration,
    timer: Timer
  ): Var[WatchState] = {
    import SessionState._
    val closedCounter = statsReceiver.counter(Closed.name)
    val unknownCounter = statsReceiver.counter(Unknown.name)
    val authFailedCounter = statsReceiver.counter(AuthFailed.name)
    val disconnectedCounter = statsReceiver.counter(Disconnected.name)
    val expiredCounter = statsReceiver.counter(Expired.name)
    val syncConnectedCounter = statsReceiver.counter(SyncConnected.name)
    val noSyncConnectedCounter = statsReceiver.counter(NoSyncConnected.name)
    val connectedReadOnlyCounter = statsReceiver.counter(ConnectedReadOnly.name)
    val saslAuthenticatedCounter = statsReceiver.counter(SaslAuthenticated.name)

    Var.async[WatchState](WatchState.Pending) { v =>
      val stateTracker = new StateTracker(statsReceiver, interval, timer)

      underlying.changes.respond { w: WatchState =>
        w match {
          case WatchState.SessionState(newState) =>
            stateTracker.transition(newState)

            newState match {
              case Closed => closedCounter.incr()
              case Unknown => unknownCounter.incr()
              case AuthFailed => authFailedCounter.incr()
              case Disconnected => disconnectedCounter.incr()
              case Expired => expiredCounter.incr()
              case SyncConnected => syncConnectedCounter.incr()
              case NoSyncConnected => noSyncConnectedCounter.incr()
              case ConnectedReadOnly => connectedReadOnlyCounter.incr()
              case SaslAuthenticated => saslAuthenticatedCounter.incr()
            }
          case _ => ()
        }
        v() = w
      }

      stateTracker
    }
  }
}
