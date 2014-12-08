package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.{DefaultTimer, Rng}
import com.twitter.io.Buf
import com.twitter.util._

/**
 * A representation of a ZooKeeper session based on asynchronous primitives such
 * as [[com.twitter.util.Future]], and [[com.twitter.util.Var]], and
 * [[com.twitter.util.Activity]].
 *
 * Session operations are as in Apache Zookeeper, but represents pending results
 * with [[com.twitter.util.Future Futures]]; watches and session states are
 * represented with a [[com.twitter.util.Var]].
 */
private class ZkSession(watchedZk: Watched[ZooKeeperReader])(implicit timer: Timer) {
  import ZkSession.randomizedDelay

  /** The dynamic `WatchState` of this `ZkSession` instance. */
  val state: Var[WatchState] = watchedZk.state

  private val zkr: ZooKeeperReader = watchedZk.value

  private def retryBackoffs =
    (Backoff.exponential(10.milliseconds, 2) take 3) ++ Backoff.const(1.second)

  /**
   * Invoke a `Future[T]`-producing operation, retrying on
   * [[com.twitter.finagle.serverset2.client.KeeperException.ConnectionLoss]]
   * according to a backoff schedule defined by [[retryBackoffs]].
   */
  private def safeRetry[T](go: => Future[T]): Future[T] = {
    def loop(backoffs: Stream[Duration]): Future[T] = go rescue {
      case exc: KeeperException.ConnectionLoss =>
        backoffs match {
          case wait #:: rest =>
            Future.sleep(randomizedDelay(wait)) before loop(rest)
          case _ => Future.exception(exc)
        }
    }

    loop(retryBackoffs)
  }

  /**
   * A persistent operation: reissue a watched operation every
   * time the watch fires, applying safe retries when possible.
   *
   * The returned Activity is asynchronous: watches aren't reissued
   * when the Activity is no longer observed.
   */
  private[serverset2] def watchedOperation[T](go: => Future[Watched[T]]): Activity[T] =
    Activity(Var.async[Activity.State[T]](Activity.Pending) { u =>
      @volatile var closed = false

      def loop() {
        if (!closed) safeRetry(go) respond {
          case Throw(exc) =>
            u() = Activity.Failed(exc)

          case Return(Watched(value, state)) =>
            val ok = Activity.Ok(value)
            u() = ok

            state.changes respond {
              case WatchState.Pending =>
                // Ignore updates WatchState is Pending.

              case WatchState.Determined(_) =>
                // Note: since the watch transitioned to determined, we know
                // that this observation will produce no more values, so there's
                // no need to apply concurrency control to the subsequent
                // branches.
                loop()

              // This should handle SaslAuthenticated in the future (if this
              // is propagated to non-session watchers).
              case WatchState.SessionState(SessionState.SyncConnected)
                 | WatchState.SessionState(SessionState.ConnectedReadOnly) =>
                u() = ok

              case WatchState.SessionState(SessionState.Expired) =>
                u() = Activity.Failed(new Exception("session expired"))

              // Disconnected, NoSyncConnected, AuthFailed
              case WatchState.SessionState(state) =>
                u() = Activity.Failed(new Exception("" + state))
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
     watchedOperation { zkr.existsWatch(path) }
   }

   private val getChildrenWatchOp = Memoize { path: String =>
     watchedOperation { zkr.getChildrenWatch(path) }
   }

  /**
   * A persistent version of exists: existsOf returns an Activity representing
   * the current (best-effort) Stat for the given path.
   */
  def existsOf(path: String): Activity[Option[Data.Stat]] =
    existsWatchOp(path)

  /**
   * A persistent version of glob: globOf returns an Activity
   * representing the current (best-effort) list of children for the
   * given path, under the given prefix. Note that paths returned are
   * absolute.
   */
  def globOf(pattern: String): Activity[Seq[String]] = {
    val slash = pattern.lastIndexOf('/')
    if (slash < 0)
      return Activity.exception(new IllegalArgumentException("Invalid pattern"))

    val (path, prefix) = ZooKeeperReader.patToPathAndPrefix(pattern)
    existsOf(path) flatMap {
      case None => Activity.value(Seq.empty)
      case Some(_) =>
        getChildrenWatchOp(path) transform {
          case Activity.Pending => Activity.pending
          case Activity.Ok(Node.Children(children, _)) =>
            Activity.value(children.filter(_.startsWith(prefix)).map(path + "/" + _))
          case Activity.Failed(KeeperException.NoNode(_)) => Activity.value(Seq.empty)
          case Activity.Failed(exc) => Activity.exception(exc)
        }
    }
  }

  private val immutableDataOf_ = Memoize { path: String =>
    Activity(Var.async[Activity.State[Option[Buf]]](Activity.Pending) { u =>
      safeRetry(zkr.getData(path)) respond {
        case Return(Node.Data(Some(data), _)) => u() = Activity.Ok(Some(data))
        case Return(_) => u() = Activity.Ok(None)
        case Throw(exc) => u() = Activity.Ok(None)
      }

      Closable.nop
    })
  }

  /**
   * A persistent version of getData: immutableDataOf returns an Activity
   * representing the current (best-effort) contents of the given
   * path. Note: this only works on immutable nodes. I.e. it does not
   * leave a watch on the node to look for changes.
   */
  def immutableDataOf(path: String): Activity[Option[Buf]] =
    immutableDataOf_(path)

  /**
   * Collect immutable data from a number of paths together.
   */
  def collectImmutableDataOf(paths: Seq[String]): Activity[Seq[(String, Option[Buf])]] = {
    def pathDataOf(path: String): Activity[(String, Option[Buf])] =
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
  def close() = zkr.close()
}

private[serverset2] object ZkSession {
  /** A noop ZkSession. */
  val nil: ZkSession = {
    implicit val timer = Timer.Nil
    new ZkSession(Watched(NullZooKeeperReader, Var(WatchState.Pending)))
  }

  val DefaultSessionTimeout = 10.seconds

  private val authUser = Identities.get().headOption getOrElse(("/null"))
  private val authInfo: String = "%s:%s".format(authUser, authUser)
  private def randomizedDelay(minDelay: Duration): Duration =
    minDelay + Duration.fromMilliseconds(Rng.threadLocal.nextInt(minDelay.inMilliseconds.toInt))

  /**
   * Produce a new `ZkSession`.
   *
   * @param hosts A comma-separated "host:port" string for a ZooKeeper server.
   * @sessionTimeout The ZooKeeper session timeout to use.
   */
  private[serverset2] def apply(
    hosts: String,
    sessionTimeout: Duration = DefaultSessionTimeout
  )(implicit timer: Timer): ZkSession = new ZkSession(
    ClientBuilder()
      .hosts(hosts)
      .sessionTimeout(sessionTimeout)
      .readOnlyOK()
      .reader()
  )

  /**
   * Produce a `Var[ZkSession]` representing a ZooKeeper session that automatically
   * reconnects upon session expiry. Reconnect attempts cease when any
   * observation of the returned `Var[ZkSession]` is closed.
   */
  def retrying(
    backoff: Duration,
    newZkSession: () => ZkSession
  )(implicit timer: Timer): Var[ZkSession] = Var.async(nil) { u =>
    @volatile var closing = false
    @volatile var zkSession: ZkSession = ZkSession.nil

    def reconnect() {
      if (closing) return

      zkSession.close()
      zkSession = newZkSession()

      // Upon initial connection, send auth info, then update `u`.
      zkSession.state.changes.filter {
        _ == WatchState.SessionState(SessionState.SyncConnected)
      }.toFuture.unit before zkSession.addAuthInfo("digest", Buf.Utf8(authInfo)) onSuccess { _ =>
        u() = zkSession
      }

      // Kick off a delayed reconnection on session expiration.
      zkSession.state.changes.filter {
        _ == WatchState.SessionState(SessionState.Expired)
      }.toFuture.unit before Future.sleep(randomizedDelay(backoff)) ensure reconnect()
    }

    reconnect()

    Closable.make { deadline =>
      closing = true
      zkSession.close()
    }
  }
}
