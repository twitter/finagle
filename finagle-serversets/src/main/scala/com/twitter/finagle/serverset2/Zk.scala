package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.{DefaultTimer, Rng}
import com.twitter.io.Buf
import com.twitter.util._

/**
 * Zk represents a ZK session.  Session operations are as in Apache
 * Zookeeper, but represents pending results with
 * [[com.twitter.util.Future Futures]]; watches and session states
 * are represented with a [[com.twitter.util.Var Var]].
 */
private class Zk(watchedZk: Watched[ZooKeeperReader], timerIn: Timer) {
  import Zk.randomizedDelay

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
            Future.sleep(randomizedDelay(wait)) before safeRetry(go, rest)
          case _ =>
            Future.exception(exc)
        }
    }

  /**
   * A persistent operation: reissue a watched operation every
   * time the watch fires, applying safe retries when possible.
   *
   * The returned Activity is asynchronous: watches aren't reissued
   * when the Activity is no longer observed.
   */
  private def op[T](which: String, arg: String)(go: => Future[Watched[T]]): Activity[T] =
    Activity(Var.async[Activity.State[T]](Activity.Pending) { v =>
      @volatile var closed = false
      def loop() {
        if (!closed) safeRetry(go, retryBackoffs) respond {
          case Throw(exc) =>
            v() = Activity.Failed(exc)
          case Return(Watched(value, state)) =>
            val ok = Activity.Ok(value)
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
                v() = Activity.Failed(new Exception("session expired"))

              // Disconnected, NoSyncConnected, AuthFailed
              case WatchState.SessionState(state) =>
                v() = Activity.Failed(new Exception("" + state))
            }
        }
      }

      loop()

      Closable.make { deadline =>
        closed = true
        Future.Done
      }
    })

   private val existsWatchOp = Memoize { path: String =>
     op("existsOf", path) { zkr.existsWatch(path) }
   }

   private val globPrefixWatchOp = Memoize[String, Activity[Seq[String]]] { pat =>
       op("globPrefixWatchOp", pat) { zkr.globPrefixWatch(pat) }
   }

  def close() = zkr.close()

  /**
   * A persistent version of exists: existsOf returns a Activity representing
   * the current (best-effort) Stat for the given path.
   */
  def existsOf(path: String): Activity[Option[Data.Stat]] =
    existsWatchOp(path)

  /**
   * A persistent version of glob: globOf returns a Activity
   * representing the current (best-effort) list of children for the
   * given path, under the given prefix. Note that paths returned are
   * absolute.
   */
  def globOf(pat: String): Activity[Seq[String]] = {
    val slash = pat.lastIndexOf('/')
    if (slash < 0)
      return Activity.exception(new IllegalArgumentException("Invalid pattern"))

    val path = if (slash == 0) "/" else pat.substring(0, slash)
    existsOf(path) flatMap {
      case None => Activity.value(Seq.empty)
      case Some(_) =>
        globPrefixWatchOp(pat) transform {
          case Activity.Pending => Activity.pending
          case Activity.Ok(paths) => Activity.value(paths)
          // This can happen when exists() races with getChildren.
          case Activity.Failed(KeeperException.NoNode(_)) => Activity.value(Seq.empty)
          case Activity.Failed(exc) => Activity.exception(exc)
        }
    }
  }

  private val immutableDataOf_ = Memoize { path: String =>
    Activity(Var.async[Activity.State[Buf]](Activity.Pending) { v =>
      safeRetry(zkr.getData(path), retryBackoffs) respond {
        case Throw(exc) => v() = Activity.Failed(exc)
        case Return(Node.Data(Some(data), _)) => v() = Activity.Ok(data)
        case Return(_) => v() = Activity.Ok(Buf.Empty)
      }

      Closable.nop
    })
  }

  /**
   * A persistent version of getData: immutableDataOf returns a Activity
   * representing the current (best-effort) contents of the given
   * path. Note: this only works on immutable nodes. I.e. it does not
   * leave a watch on the node to look for changes.
   */
  def immutableDataOf(path: String): Activity[Buf] =
    immutableDataOf_(path)

  /**
   * Collect immutable data from a number of paths together.
   */
  def collectImmutableDataOf(paths: Seq[String]): Activity[Seq[(String, Buf)]] = {
    def pathDataOf(path: String): Activity[(String, Buf)] =
      immutableDataOf(path).map(path -> _)

    Activity.collect(paths map pathDataOf)
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
  
  def globPrefixWatch(pat: String): Future[Watched[Seq[String]]] = Future.never

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
      ClientBuilder()
        .hosts(hosts)
        .sessionTimeout(timeout)
        .readOnlyOK()
        .reader(),
      DefaultTimer.twitter)) {

  val user: String = System.getProperty("user.name", "unknown")
  val authInfo: String = "%s:%s".format(user, user)
  val nil: Zk = new Zk(Watched(new NullZooKeeperReader, Var(WatchState.Pending)), Timer.Nil)

  private def randomizedDelay(minDelay: Duration): Duration =
    minDelay + Duration.fromMilliseconds(Rng.threadLocal.nextInt(minDelay.inMilliseconds.toInt))

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
      val ready = zk.state.changes.filter(_ == WatchState.SessionState(SessionState.SyncConnected))
      ready.toFuture().onSuccess { _ =>
        zk.addAuthInfo("digest", Buf.Utf8(authInfo))
      }
      val expired = zk.state.changes.filter(_ == WatchState.SessionState(SessionState.Expired))
      expired.toFuture().onSuccess { _ =>
        timer.doLater(randomizedDelay(backoff)) {
          reconnect()
        }
      }
      u() = zk
    }

    reconnect()

    Closable.make { deadline =>
      closing = true
      zk.close()
    }
  }
}
