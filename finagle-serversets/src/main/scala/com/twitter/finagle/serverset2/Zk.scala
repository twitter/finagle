package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.util.{Closable, Var,  Future, Promise, Duration, Return, Throw, Timer, Memoize}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{ZooKeeper, KeeperException, WatchedEvent, Watcher}

/**
 * The state of a ZK watch starts out being Pending. It may
 * transition to SessionState multiple times, and finally to
 * Determined. (Unless it fails on session expiry.)
 */
private sealed trait WatchState
private object WatchState {
  object Pending extends WatchState
  object Determined extends WatchState
  case class SessionState(state: KeeperState) extends WatchState
}

/**
 * Op represents a ZK operation: it is either Pending, Ok, or Failed.
 */
private sealed trait Op[+T] { def map[U](f: T => U): Op[U] }
private object Op {
  object Pending extends Op[Nothing] { def map[U](f: Nothing => U) = this }
  case class Ok[T](v: T) extends Op[T] { def map[U](f: T => U) = Ok(f(v)) }
  case class Fail(e: Throwable) extends Op[Nothing] { def map[U](f: Nothing => U) = this }

  def flatMap[T, U](v: Var[Op[T]])(f: T => Var[Op[U]]): Var[Op[U]] =
    v flatMap {
      case Op.Pending => Var.value(Op.Pending)
      case op@Op.Fail(_) => Var.value(op)
      case Op.Ok(t) => f(t)
    }

  def map[T, U](v: Var[Op[T]])(f: T => U): Var[Op[U]] =
    flatMap(v) { t => Var.value(Op.Ok(f(t))) }
}

/**
 * Zk represents a ZK session.  Session operations are as in Apache
 * Zookeeper, but represents pending results with
 * [[com.twitter.util.Future Futures]]; watches and session states
 * are represented with a [[com.twitter.util.Var Var]].
 */
private trait Zk {
  type Watched[T] = (T, Var[WatchState])

  def exists(path: String): Future[Option[Stat]]
  def existsWatch(path: String): Future[Watched[Option[Stat]]]

  def getChildren(path: String): Future[Seq[String]]
  def getChildrenWatch(path: String): Future[Watched[Seq[String]]]

  def getData(path: String): Future[(Stat, Buf)]
  def getDataWatch(path: String): Future[Watched[(Stat, Buf)]]

  def sync(path: String): Future[Unit]
  def close(): Future[Unit]

  def sessionId: Long
  def sessionPasswd: Buf
  def sessionTimeout: Duration

  /**
   * Represents the session state.
   */
  val state: Var[WatchState]

  protected[serverset2] implicit val timer: Timer

  protected def retryBackoffs = 
    (Backoff.exponential(10.milliseconds, 2) take 3) ++ Backoff.const(1.second)

  private def safeRetry[T](go: => Future[T], backoff: Stream[Duration])
      (implicit timer: Timer): Future[T] = 
    go rescue {
      case exc: KeeperException.ConnectionLossException =>
        backoff match {
          case wait #:: rest =>
            Future.sleep(wait) before safeRetry(go, rest)
          case _ => 
            Future.exception(exc)
        }
  }

  /**
   * A persistent operation: reissue a watched operation every
   * time the watch fires, applying safe retries when possible.
   *
   * The returned Var is asynchronous: watches aren't reissued
   * when the Var is no longer observed.
   */
  private def op[T](which: String, arg: String)(go: => Future[Watched[T]]): Var[Op[T]] = 
    Var.async(Op.Pending: Op[T]) { v =>
      @volatile var closed = false
      def loop(): Unit =
        if (!closed) safeRetry(go, retryBackoffs) respond {
          case Throw(exc) => 
            v() = Op.Fail(exc)
          case Return((stat, w)) =>
            val ok = Op.Ok(stat)
            v() = ok
            w observe {
              case WatchState.Pending =>

              case WatchState.Determined => 
                // Note: since the watch transitioned to determined, we know
                // that this observation will produce no more values, so there's
                // no need to apply concurrency control to the subsequent 
                // branches.
                loop()

              // Note: ReadOnly mode isn't yet available. When it is, uncomment
              // this line.
              //case WatchState.SessionState(KeeperState.ConnectedReadOnly) =>s

              case WatchState.SessionState(KeeperState.Expired) =>
                v() = Op.Fail(new Exception("session expired"))
                
              // This should handle SaslAuthenticated in the future (if this
              // is propagated to non-session watchers).
              case WatchState.SessionState(KeeperState.SyncConnected) =>
                v() = ok

              // Disconnected, NoSyncConnected, AuthFailed
              case WatchState.SessionState(state) =>
                v() = Op.Fail(new Exception(""+state))
            }
        }

      loop()

      Closable.make { deadline =>
        closed = true
        Future.Done
      }
    }
   
   private val existsWatchOp = Memoize { path: String =>
     op("existsOf", path) { existsWatch(path) }
   }
   
   private val childrenWatchOp = Memoize { path: String =>
     op("childrenWatchOp", path) { getChildrenWatch(path) }
   }

  /**
   * A persistent version of exists: existsOf returns a Var representing
   * the current (best-effort) Stat for the given path.
   */
  def existsOf(path: String): Var[Op[Option[Stat]]] =
    existsWatchOp(path)

  /**
   * A persistent version of getChildren: childrenOf returns a Var
   * representing the current (best-effort) list of children for the
   * given path.
   */
  def childrenOf(path: String): Var[Op[Set[String]]] =
    Op.flatMap(existsOf(path)) {
      case None => Var.value(Op.Ok(Set.empty))
      case Some(_) =>
        childrenWatchOp(path) flatMap {
          case Op.Pending => Var.value(Op.Pending)
          case Op.Ok(children) => Var.value(Op.Ok(children.toSet))
          // This can happen when exists() races with getChildren.
          case Op.Fail(exc: KeeperException)
          if exc.code == KeeperException.Code.NONODE =>
            Var.value(Op.Ok(Set.empty))
          case f@Op.Fail(_) => Var.value(f)
        }
    }

  private val immutableDataOf_ = Memoize { path: String =>
    Var.async(Op.Pending: Op[Buf]) { v =>
      safeRetry(getData(path), retryBackoffs) respond {
        case Throw(exc) => v() = Op.Fail(exc)
        case Return((_, buf)) => v() = Op.Ok(buf)
      }
      
      Closable.nop
    }
  }

  /**
   * A persistent version of getData: immutableDataOf returns a Var
   * representing the current (best-effort) contents of the given
   * path. Note: this only works on immutable nodes. I.e. it does not
   * leave a watch on the node to look for changes.
   */
  def immutableDataOf(path: String): Var[Op[Buf]] =
    immutableDataOf_(path)

  /**
   * Collect immutable data from a number of paths together.
   */
  def collectImmutableDataOf(paths: Set[String]): Var[Op[Map[String, Buf]]] = {
    def pathDataOf(path: String): Var[Op[(String, Buf)]] =
      immutableDataOf(path) map {
        op => op map { data => path -> data }
      }

    Var.collect(paths map pathDataOf) map {
      // TODO: be more eager here?
      case ops if ops exists (_ == Op.Pending) => Op.Pending
      case ops =>
        val pathData = ops collect {
          case Op.Ok(pathData@(_, data)) if data != null => pathData
        }
        Op.Ok(pathData.toMap)
    }
  }
}

private trait ZkFactory {
  def apply(hosts: String): Zk
  def withTimeout(d: Duration): ZkFactory
  def purge(zk: Zk)
}

private class FnZkFactory(
    newZk: (String, Duration) => Zk, 
    timeout: Duration = 3.seconds) extends ZkFactory {

  def apply(hosts: String): Zk = newZk(hosts, timeout)
  def withTimeout(d: Duration) = new FnZkFactory(newZk, d)
  def purge(zk: Zk) = ()
}

private object Zk extends FnZkFactory(
    (hosts, timeout) => new ZooKeeperZk(w => 
      new ZooKeeper(hosts, timeout.inMilliseconds.toInt, w))) {

  val nil: Zk = new Zk {
    protected[serverset2] implicit val timer = Timer.Nil

    def exists(path: String): Future[Option[Stat]] = Future.never
    def existsWatch(path: String): Future[Watched[Option[Stat]]] = Future.never
  
    def getChildren(path: String): Future[Seq[String]] = Future.never
    def getChildrenWatch(path: String): Future[Watched[Seq[String]]] = Future.never
  
    def getData(path: String): Future[(Stat, Buf)] = Future.never
    def getDataWatch(path: String): Future[Watched[(Stat, Buf)]] = Future.never
  
    def sync(path: String): Future[Unit] = Future.never
    def close(): Future[Unit] = Future.never

    def sessionId: Long = -1
    def sessionPasswd: Buf = Buf.Empty
    def sessionTimeout: Duration = 0.seconds
  
    /**
     * Represents the session state.
     */
    val state: Var[WatchState] = Var.value(WatchState.Pending)
  }

  private[finagle] def newWatcher() = new Watcher {
    private val p = new Promise[WatchedEvent]
    val state = Var[WatchState](WatchState.Pending)
    def process(event: WatchedEvent) = {
      if (event.getType == EventType.None)
        state() = WatchState.SessionState(event.getState())
      else
        state() = WatchState.Determined
      }
  }

  def retrying(
      backoff: Duration,
      newZk: () => Zk, 
      timer: Timer = DefaultTimer.twitter): Var[Zk] = Var.async(nil) { u =>
    @volatile var closing = false
    @volatile var zk: Zk = Zk.nil

    def reconnect() {
      if (closing)
        return
      zk.close()
      zk = newZk()
      u() = zk
      val whenUnhealthy = 
        zk.state.observeUntil(_ == WatchState.SessionState(KeeperState.Expired))
      whenUnhealthy onSuccess { _ =>
        timer.doLater(backoff) {
          reconnect()
        }
      }
    }

    reconnect()

    Closable.make { deadline =>
      closing = true
      zk.close()
    }
  }
}
