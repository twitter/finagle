package com.twitter.finagle.serverset2

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.stats.{DefaultStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.io.Buf
import com.twitter.logging.Logger
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
private[serverset2] class ZkSession(
    watchedZk: Watched[ZooKeeperReader],
    statsReceiver: StatsReceiver
  )(implicit timer: Timer) {
  import ZkSession.logger

  /** The dynamic `WatchState` of this `ZkSession` instance. */
  val state: Var[WatchState] = watchedZk.state

  private val zkr: ZooKeeperReader = watchedZk.value

  private def retryBackoffs =
    (Backoff.decorrelatedJittered(10.milliseconds, 1.second) take 3) ++ Backoff.const(1.second)

  // If the zookeeper cluster is under duress, there can be 100's of thousands of clients
  // attempting to read and write at once. Limit to a (fairly large) concurrent request cap.
  // Use a semaphore (versus explicit rate limiting) to approximate the throughput of the cluster.
  // N.B. this semaphore has no max-waiters limit. This could lead to an OOME if the zk operations
  // never complete. This is preferable to handling and re-queuing (via future.sleep etc)
  // the error if an arbitrary max-limit is set.
  private val limiter = new AsyncSemaphore(100)
  private val waitersGauge = statsReceiver.addGauge("numWaiters") { limiter.numWaiters }

  private def limit[T](f: => Future[T]): Future[T] =
    limiter.acquire().flatMap { permit =>
      f.ensure {
        // don't release the permit until f is complete
        permit.release()
      }
    }

  /**
   * Invoke a `Future[T]`-producing operation, retrying on
   * [[com.twitter.finagle.serverset2.client.KeeperException.ConnectionLoss]]
   * according to a backoff schedule defined by [[retryBackoffs]]. The operation itself
   * will be limited by the session-level semaphore.
   */
  private def safeRetry[T](go: => Future[T]): Future[T] = {
    def loop(backoffs: Stream[Duration]): Future[T] =
      limit { go }.rescue {
        case exc: KeeperException.ConnectionLoss =>
          backoffs match {
            case wait #:: rest =>
              logger.warning(s"ConnectionLoss to Zookeeper host. Retrying in $wait")
              Future.sleep(wait) before loop(rest)
            case _ =>
              logger.error(s"ConnectionLoss. Out of retries - failing request with $exc")
              Future.exception(exc)
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
            logger.error(s"Operation failed with $exc")
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
                logger.error("Session has expired. Watched data is now unavailable.")
                u() = Activity.Failed(new Exception("session expired"))

              // Disconnected, NoSyncConnected, AuthFailed
              case WatchState.SessionState(state) =>
                logger.error(s"Failure session state: $state. Data is now unavailable.")
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
  def globOf(pattern: String): Activity[Set[String]] = {
    val slash = pattern.lastIndexOf('/')
    if (slash < 0)
      return Activity.exception(new IllegalArgumentException("Invalid pattern"))

    val (path, prefix) = ZooKeeperReader.patToPathAndPrefix(pattern)
    existsOf(path) flatMap {
      case None => Activity.value(Set.empty)
      case Some(_) =>
        getChildrenWatchOp(path) transform {
          case Activity.Pending => Activity.pending
          case Activity.Ok(Node.Children(children, _)) =>
            Activity.value(children.filter(_.startsWith(prefix)).toSet)
          case Activity.Failed(KeeperException.NoNode(_)) => Activity.value(Set.empty)
          case Activity.Failed(exc) =>
            logger.error(s"GetChildrenWatch to ($path, $prefix) failed with exception $exc")
            Activity.exception(exc)
        }
    }
  }

  /**
   * A persistent version of getData: immutableDataOf returns a Future
   * representing the current (best-effort) contents of the given
   * path. Note: this only works on immutable nodes. I.e. it does not
   * leave a watch on the node to look for changes.
   */
  def immutableDataOf(path: String): Future[Option[Buf]] =
    safeRetry(zkr.getData(path)).transform {
      case Return(Node.Data(Some(data), _)) =>
        logger.debug(s"Zk.GetData($path) retrieved ${data.length} bytes")
        Future.value(Some(data))
      case Return(_) => Future.value(None)
      case Throw(exc) => Future.value(None)
    }

  /**
   * Collect immutable data from a number of paths together.
   */
  def collectImmutableDataOf(paths: Seq[String]): Future[Seq[(String, Option[Buf])]] = {
    def pathDataOf(path: String): Future[(String, Option[Buf])] =
      immutableDataOf(path).map(path -> _)

    Future.collect(paths map pathDataOf)
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
    new ZkSession(Watched(NullZooKeeperReader, Var(WatchState.Pending)), NullStatsReceiver)
  }

  val DefaultSessionTimeout = 10.seconds
  val DefaultBackoff = 5.seconds

  private val authUser = Identities.get().headOption getOrElse(("/null"))
  private val authInfo: String = "%s:%s".format(authUser, authUser)
  private val logger = Logger("ZkSession")

  /**
   * Produce a new `ZkSession`.
   *
   * @param hosts A comma-separated "host:port" string for a ZooKeeper server.
   * @param sessionTimeout The ZooKeeper session timeout to use.
   */
  private[serverset2] def apply(
    hosts: String,
    sessionTimeout: Duration = DefaultSessionTimeout,
    statsReceiver: StatsReceiver
  )(implicit timer: Timer): ZkSession =
    new ZkSession(ClientBuilder()
        .hosts(hosts)
        .sessionTimeout(sessionTimeout)
        .statsReceiver(DefaultStatsReceiver.scope("zkclient").scope(Zk2Resolver.statsOf(hosts)))
        .readOnlyOK()
        .reader(),
      statsReceiver.scope(Zk2Resolver.statsOf(hosts)))

  /**
   * Produce a `Var[ZkSession]` representing a ZooKeeper session that automatically
   * reconnects upon session expiry. Reconnect attempts cease when any
   * observation of the returned `Var[ZkSession]` is closed.
   */
  def retrying(
      backoff: Stream[Duration],
      newZkSession: () => ZkSession
  )(implicit timer: Timer): Var[ZkSession] = {
    val v = Var(ZkSession.nil)

    @volatile var closing = false
    @volatile var zkSession: ZkSession = ZkSession.nil
    @volatile var remainingBackoff = backoff

    def nextBackoff(): Duration = {
      remainingBackoff match {
        case value #:: rest =>
          remainingBackoff = rest
          value

        case _ => DefaultBackoff
      }
    }
    def reconnect() {
      if (closing) return

      zkSession.close()
      zkSession = newZkSession()

      // Upon initial connection, send auth info, then update `u`.
      zkSession.state.changes.filter {
        _ == WatchState.SessionState(SessionState.SyncConnected)
      }.toFuture.unit before zkSession.addAuthInfo("digest", Buf.Utf8(authInfo)) onSuccess { _ =>
        v() = zkSession
      }

      // Kick off a delayed reconnection on session expiration.
      zkSession.state.changes.filter {
        _ == WatchState.SessionState(SessionState.Expired)
      }.toFuture().unit.before {
        val jitter = nextBackoff()
        logger.error(s"Zookeeper session has expired. Reconnecting in $jitter")
        Future.sleep(jitter)
      }.ensure { reconnect() }
    }

    reconnect()

    Closable.make { deadline =>
      closing = true
      zkSession.close()
    }
    v
  }
}
