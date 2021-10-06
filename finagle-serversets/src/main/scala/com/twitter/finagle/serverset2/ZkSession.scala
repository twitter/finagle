package com.twitter.finagle.serverset2

import com.twitter.app.GlobalFlag
import com.twitter.concurrent.AsyncSemaphore
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.stats._
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import java.net.UnknownHostException
import scala.collection.concurrent

object zkConcurrentOperations
    extends GlobalFlag[Int](
      100,
      "Number of concurrent operations allowed per ZkClient (default 100, max 1000). " +
        "Ops exceeding this limit will be queued until existing operations complete."
    )

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
  retryStream: RetryStream,
  watchedZk: Watched[ZooKeeperReader],
  statsReceiver: StatsReceiver
)(
  implicit timer: Timer) {
  import ZkSession.logger

  /** The dynamic `WatchState` of this `ZkSession` instance. */
  val state: Var[WatchState] = watchedZk.state

  private[this] val unexpectedExceptions = new CategorizingExceptionStatsHandler(_ =>
    Some("unexpected_exceptions"))

  private val zkr: ZooKeeperReader = watchedZk.value

  // If the zookeeper cluster is under duress, there can be 100's of thousands of clients
  // attempting to read and write at once. Limit to a (fairly large) concurrent request cap.
  // Use a semaphore (versus explicit rate limiting) to approximate the throughput of the cluster.
  // N.B. this semaphore has no max-waiters limit. This could lead to an OOME if the zk operations
  // never complete. This is preferable to handling and re-queuing (via future.sleep etc)
  // the error if an arbitrary max-limit is set.
  private[this] val numPermits = {
    val flagVal = zkConcurrentOperations()
    if (flagVal > 0 && flagVal <= ZkSession.MaxPermits) flagVal else ZkSession.DefaultPermits
  }

  private val limiter = new AsyncSemaphore(numPermits)
  private val waitersGauge = statsReceiver.addGauge("numWaiters") { limiter.numWaiters }

  private def limit[T](f: => Future[T]): Future[T] =
    limiter.acquire().flatMap { permit =>
      f.ensure {
        // don't release the permit until f is complete
        permit.release()
      }
    }

  private def retryWithDelay[T](f: => Future[T]): Future[T] =
    Future.sleep(retryStream.next()).before(f)

  // Track a timestamp for the last time we received a good update for
  // a particular zookeeper child watch. All servers should be updated within the same approximate
  // time. If a server has a different serverset size than its peers, this gauge will show
  // us it is because it is not receiving updates.
  @volatile var watchUpdateGauges = List.empty[Gauge]
  private val lastGoodUpdate = new concurrent.TrieMap[String, Long]
  private def noteGoodChildWatch(path: String): Unit = {
    lastGoodUpdate.put(path, Time.now.inLongSeconds) match {
      case None =>
        // if there was no previous value, ensure we have a gauge
        synchronized {
          watchUpdateGauges ::= statsReceiver.addGauge("last_watch_update", path) {
            Time.now.inLongSeconds - lastGoodUpdate.getOrElse(path, 0L)
          }
        }
      case _ => //gauge is already there
    }
  }

  /**
   * Invoke a `Future[T]`-producing operation, retrying on
   * [[com.twitter.finagle.serverset2.client.KeeperException.ConnectionLoss]]
   * according to a backoff schedule defined by [[retryStream]]. The operation itself
   * will be limited by the session-level semaphore.
   */
  private def safeRetry[T](go: => Future[T]): Future[T] = {
    def loop(): Future[T] =
      limit { go }.rescue {
        case exc: KeeperException.ConnectionLoss =>
          logger.warning(s"ConnectionLoss to Zookeeper host. Session $sessionIdAsHex. Retrying")
          retryWithDelay { loop() }
      }

    loop()
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

      def loop(): Future[Unit] = {
        if (!closed) safeRetry(go).respond {
          case Throw(e @ KeeperException.SessionExpired(_)) =>
            // don't retry. The session has expired while trying to set the watch.
            // In case our activity is still active, notify the listener
            u() = Activity.Failed(e)

          case Throw(exc) =>
            logger.error(s"Operation failed with $exc. Session $sessionIdAsHex")
            u() = Activity.Failed(exc)
            retryWithDelay { loop() }

          case Return(Watched(value, state)) =>
            val ok = Activity.Ok(value)
            retryStream.reset()
            u() = ok

            state.changes.respond {
              case WatchState.Pending =>
              // Ignore updates WatchState is Pending.

              case WatchState.Determined(_) =>
                // Note: since the watch transitioned to determined, we know
                // that this observation will produce no more values, so there's
                // no need to apply concurrency control to the subsequent
                // branches.
                loop()

              case WatchState.SessionState(sessionState)
                  if sessionState == SessionState.ConnectedReadOnly |
                    sessionState == SessionState.SaslAuthenticated |
                    sessionState == SessionState.SyncConnected =>
                u() = ok
                logger.info(s"Reacquiring watch on $sessionState. Session: $sessionIdAsHex")
                // We may have lost or never set our watch correctly. Retry to ensure we stay connected
                retryWithDelay { loop() }

              case WatchState.SessionState(SessionState.Expired) =>
                u() = Activity.Failed(new Exception("session expired"))
              // Do NOT retry here as the session has expired. We expect the watcher of this
              // ZkSession to retry at this point (See [[ZkSession.retrying]]).

              // Disconnected, NoSyncConnected
              case WatchState.SessionState(sessionState)
                  if sessionState == SessionState.Disconnected |
                    sessionState == SessionState.NoSyncConnected =>
                logger.warning(
                  s"Intermediate Failure session state: $sessionState. " +
                    s"Session: $sessionIdAsHex. Data is now unavailable."
                )
                u() = Activity.Failed(new Exception("" + sessionState))
              // Do NOT keep retrying, wait to be reconnected automatically by the underlying session

              case WatchState.SessionState(SessionState.Closed) =>
              // The Closed state is introduced in ZK 3.5.5 but for version compatibility,
              // we do nothing here, adding this case to not break OSS usages for now. Would
              // be great to re-evaluate the action once ZK is upgraded to or beyond 3.5.5.
              case WatchState.SessionState(sessionState) =>
                logger.error(s"Unexpected session state $sessionState. Session: $sessionIdAsHex")
                u() = Activity.Failed(new Exception("" + sessionState))
                // We don't know what happened. Retry.
                retryWithDelay { loop() }
            }
        }
        Future.Done
      }

      loop()

      Closable.make { deadline =>
        closed = true
        Future.Done
      }
    })

  private val existsWatchOp = Memoize { path: String => watchedOperation { zkr.existsWatch(path) } }

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
            noteGoodChildWatch(path)
            Activity.value(children.filter(_.startsWith(prefix)).toSet)
          case Activity.Failed(KeeperException.NoNode(_)) =>
            noteGoodChildWatch(path)
            Activity.value(Set.empty)
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
      case Throw(ex: KeeperException.NoNode) => Future.value(None)
      case Throw(exc) =>
        statsReceiver.counter("read_fail").incr()
        unexpectedExceptions.record(statsReceiver, exc)
        logger.warning(
          s"Unexpected failure for session $sessionIdAsHex. retrieving node $path. ($exc)"
        )
        Future.exception(exc)
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
  def sessionIdAsHex = zkr.sessionId.toHexString
  def sessionPasswd: Buf = zkr.sessionPasswd
  def sessionTimeout: Duration = zkr.sessionTimeout
  def close() = zkr.close()
}

private[serverset2] object ZkSession {
  private val MaxPermits = 1000
  private val DefaultPermits = 100

  /** A noop ZkSession. */
  val nil: ZkSession = {
    implicit val timer = Timer.Nil
    new ZkSession(
      RetryStream(),
      Watched(NullZooKeeperReader, Var(WatchState.Pending)),
      NullStatsReceiver
    )
  }

  val DefaultSessionTimeout = 10.seconds

  private val authUser = Identities.get().headOption getOrElse (("/null"))
  private val authInfo: String = "%s:%s".format(authUser, authUser)
  private val logger = Logger("ZkSession")

  /**
   * Produce a new `ZkSession`.
   *
   * Note: if `ClientBuilder` throws `UnknownHostException`, we return a no-op `ZkSession`
   * in a failed state to trigger a retry (see `ZkSession#retrying`).
   *
   * @param retryStream The backoff schedule for reconnecting on session expiry and retrying operations.
   * @param hosts A comma-separated "host:port" string for a ZooKeeper server.
   * @param sessionTimeout The ZooKeeper session timeout to use.
   * @param statsReceiver A [[StatsReceiver]] for the ZooKeeper session.
   */
  private[serverset2] def apply(
    retryStream: RetryStream,
    hosts: String,
    sessionTimeout: Duration = DefaultSessionTimeout,
    statsReceiver: StatsReceiver
  )(
    implicit timer: Timer
  ): ZkSession = {
    val watchedZkReader =
      try {
        ClientBuilder()
          .hosts(hosts)
          .sessionTimeout(sessionTimeout)
          .statsReceiver(DefaultStatsReceiver.scope("zkclient").scope(Zk2Resolver.statsOf(hosts)))
          .readOnlyOK()
          .reader()
      } catch {
        case exc: UnknownHostException =>
          Watched(NullZooKeeperReader, Var(WatchState.FailedToInitialize(exc)))
      }

    new ZkSession(retryStream, watchedZkReader, statsReceiver.scope(Zk2Resolver.statsOf(hosts)))
  }

  /**
   * Determine if the given `WatchState` requires a reconnect (see `ZkSession#retrying`).
   */
  private[serverset2] def needsReconnect(state: WatchState): Boolean = {
    state match {
      case WatchState.FailedToInitialize(_) | WatchState.SessionState(SessionState.Expired) => true
      case _ => false
    }
  }

  /**
   * Produce a `Var[ZkSession]` representing a ZooKeeper session that automatically retries
   * when a session fails to initialize _or_ when the current session expires.
   *
   * Reconnect attempts cease when any observation of the returned Var[ZkSession] is closed.
   */
  def retrying(
    backoff: RetryStream,
    newZkSession: () => ZkSession
  )(
    implicit timer: Timer
  ): Var[ZkSession] = {
    val v = Var(ZkSession.nil)

    @volatile var closing = false
    @volatile var zkSession: ZkSession = ZkSession.nil

    def reconnect(): Unit = {
      if (closing) return

      logger.info(s"Closing zk session ${zkSession.sessionIdAsHex}")
      zkSession.close()
      zkSession = newZkSession()
      logger.info(s"Starting new zk session ${zkSession.sessionId}")

      // Upon initial connection, send auth info, then update `u`.
      zkSession.state.changes
        .filter {
          _ == WatchState.SessionState(SessionState.SyncConnected)
        }
        .toFuture()
        .unit.before {
          zkSession.addAuthInfo("digest", Buf.Utf8(authInfo))
        }
        .respond {
          case Return(_) =>
            logger.info(s"New ZKSession is connected. Session ID: ${zkSession.sessionIdAsHex}")
            v() = zkSession
            backoff.reset()
          case _ =>
        }

      // Kick off a delayed reconnection if the new session failed to initialize _or_ the current session expired.
      zkSession.state.changes
        .filter(needsReconnect)
        .toFuture()
        .flatMap { state =>
          val jitter = backoff.next()
          state match {
            case WatchState.FailedToInitialize(exc) =>
              logger.error(
                s"Zookeeper session failed to initialize with exception: $exc. Retrying in $jitter"
              )
            case WatchState.SessionState(SessionState.Expired) =>
              logger.error(
                s"Zookeeper session ${zkSession.sessionIdAsHex} has expired. Reconnecting in $jitter"
              )
            case _ => throw new MatchError
          }
          Future.sleep(jitter)
        }
        .ensure(reconnect())
    }

    reconnect()

    Closable.make { deadline =>
      closing = true
      zkSession.close()
    }
    v
  }
}
