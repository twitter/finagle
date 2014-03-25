package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.util._

/**
 * Op represents a ZK operation: it is either Pending, Ok, or Failed.
 */
private[serverset2] sealed trait Op[+T] { def map[U](f: T => U): Op[U] }
private[serverset2] object Op {
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
private class Zk(watchedZk: Watched[ZooKeeperReader], timerIn: Timer) {
  val state: Var[WatchState] = watchedZk.state
  protected[serverset2] implicit val timer: Timer = timerIn
  private val zkr: ZooKeeperReader = watchedZk.value

  private def retryBackoffs =
    (Backoff.exponential(10.milliseconds, 2) take 3) ++ Backoff.const(1.second)

  private def safeRetry[T](go: => Future[T], backoff: Stream[Duration])
      (implicit timer: Timer): Future[T] =
    go rescue {
      case exc: KeeperException.ConnectionLoss =>
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
      def loop() {
        if (!closed) safeRetry(go, retryBackoffs) respond {
          case Throw(exc) =>
            v() = Op.Fail(exc)
          case Return(Watched(value, state)) =>
            val ok = Op.Ok(value)
            v() = ok
            state.changes respond {
              case WatchState.Pending =>

              case WatchState.Determined(_) =>
                // Note: since the watch transitioned to determined, we know
                // that this observation will produce no more values, so there's
                // no need to apply concurrency control to the subsequent
                // branches.
                loop()

              // This should handle SaslAuthenticated in the future (if this
              // is propagated to non-session watchers).
              case WatchState.SessionState(SessionState.SyncConnected) =>
                v() = ok

              case WatchState.SessionState(SessionState.ConnectedReadOnly) =>
                v() = ok

              case WatchState.SessionState(SessionState.Expired) =>
                v() = Op.Fail(new Exception("session expired"))

              // Disconnected, NoSyncConnected, AuthFailed
              case WatchState.SessionState(state) =>
                v() = Op.Fail(new Exception("" + state))
            }
        }
      }

      loop()

      Closable.make { deadline =>
        closed = true
        Future.Done
      }
    }

   private val existsWatchOp = Memoize { path: String =>
     op("existsOf", path) { zkr.existsWatch(path) }
   }

   private val childrenWatchOp = Memoize { path: String =>
     op("childrenWatchOp", path) { zkr.getChildrenWatch(path) }
   }

  def close() = zkr.close()

  /**
   * A persistent version of exists: existsOf returns a Var representing
   * the current (best-effort) Stat for the given path.
   */
  def existsOf(path: String): Var[Op[Option[Data.Stat]]] =
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
          case Op.Ok(Node.Children(children, _)) => Var.value(Op.Ok(children.toSet))
          // This can happen when exists() races with getChildren.
          case Op.Fail(KeeperException.NoNode(_)) => Var.value(Op.Ok(Set.empty))
          case f@Op.Fail(_) => Var.value(f)
        }
    }

  private val immutableDataOf_ = Memoize { path: String =>
    Var.async(Op.Pending: Op[Buf]) { v =>
      safeRetry(zkr.getData(path), retryBackoffs) respond {
        case Throw(exc) => v() = Op.Fail(exc)
        case Return(Node.Data(Some(data), _)) => v() = Op.Ok(data)
        case Return(_) => v() = Op.Ok(Buf.Empty)
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

  def addAuthInfo(scheme: String, auth: Buf): Future[Unit] = zkr.addAuthInfo(scheme, auth)
  def existsWatch(path: String): Future[Watched[Option[Data.Stat]]] = zkr.existsWatch(path)
  def getChildrenWatch(path: String): Future[Watched[Node.Children]] = zkr.getChildrenWatch(path)
  def getData(path: String): Future[Node.Data] = zkr.getData(path)
  def sessionId: Long = zkr.sessionId
  def sessionPasswd: Buf = zkr.sessionPasswd
  def sessionTimeout: Duration = zkr.sessionTimeout
}

private class NullZooKeeperReader extends ZooKeeperReader {
  def addAuthInfo(scheme: String, auth: Buf): Future[Unit] = Future.never

  def exists(path: String): Future[Option[Data.Stat]] = Future.never
  def existsWatch(path: String): Future[Watched[Option[Data.Stat]]] = Future.never

  def getACL(path: String): Future[Node.ACL] = Future.never

  def getChildren(path: String): Future[Node.Children] = Future.never
  def getChildrenWatch(path: String): Future[Watched[Node.Children]] = Future.never

  def getData(path: String): Future[Node.Data] = Future.never
  def getDataWatch(path: String): Future[Watched[Node.Data]] = Future.never

  def sync(path: String): Future[Unit] = Future.never
  def close(deadline: Time): Future[Unit] = Future.never

  def sessionId: Long = -1
  def sessionPasswd: Buf = Buf.Empty
  def sessionTimeout: Duration = 0.seconds
}

private[serverset2] trait ZkFactory {
  def apply(hosts: String): Zk
  def withTimeout(d: Duration): ZkFactory
  def purge(zk: Zk)
}

private[serverset2] class FnZkFactory(
    newZk: (String, Duration) => Zk,
    timeout: Duration = 3.seconds) extends ZkFactory {

  def apply(hosts: String): Zk = newZk(hosts, timeout)
  def withTimeout(d: Duration) = new FnZkFactory(newZk, d)
  def purge(zk: Zk) = ()
}

private[serverset2] object Zk extends FnZkFactory(
    (hosts, timeout) => new Zk(
      ClientBuilder().hosts(hosts).sessionTimeout(timeout).readOnlyOK().reader(),
      DefaultTimer.twitter)) {

  val nil: Zk = new Zk(Watched(new NullZooKeeperReader, Var(WatchState.Pending)), Timer.Nil)

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
        zk.state.changes.filter { _ == WatchState.SessionState(SessionState.Expired) }.toFuture()
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
