package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client.{SessionState, WatchState}
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.stats.{Gauge, Stat, StatsReceiver}
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._


private[serverset2] object ServiceDiscoverer {
  val DefaultRetrying = Backoff.decorrelatedJittered(5.seconds, 30.seconds)
  val EndpointGlob = "/member_"
  val VectorGlob = "/vector_"

  /**
   * Compute weights for a set of ServerSet entries according to a set of
   * weight vectors.
   *
   * Each entry in `ents` is paired with the product of all weights for that
   * entry in `vecs`.
   */
  def zipWithWeights(ents: Seq[Entry], vecs: Set[Vector]): Seq[(Entry, Double)] = {
    ents map { ent =>
      val w = vecs.foldLeft(1.0) { case (w, vec) => w*vec.weightOf(ent) }
      ent -> w
    }
  }

  /**
   * ZooKeeper client health as observed by the ServiceDiscoverer.
   */
  private[serverset2] sealed trait ClientHealth
  private[serverset2] object ClientHealth {
    case object Healthy extends ClientHealth
    case object Unhealthy extends ClientHealth

    def apply(sessionState: SessionState): ClientHealth = {
      sessionState match {
        case SessionState.Expired | SessionState.NoSyncConnected
             | SessionState.Unknown | SessionState.AuthFailed
             | SessionState.Disconnected => Unhealthy
        case SessionState.ConnectedReadOnly | SessionState.SaslAuthenticated
            | SessionState.SyncConnected => Healthy
      }
    }
  }

}

/**
 * A representation of a session to a given ZooKeeper-backed service
 * discovery cluster.
 *
 * Given a ServerSet path, [[apply]] looks up the set and returns a
 * dynamic set of (entry, weight) tuples.
 * @param varZkSession: The active, connected zkSession. This session
 *    may change in response to normal zookeeper changes
 *    (such as servers restarting).
 * @param statsReceiver: Scoped statsReceiver
 * @param healthStabilizationEpoch: Used in stabilizing the reporting
 *  health changes of the underlying ZkSession
 */
private[serverset2] class ServiceDiscoverer(
  varZkSession: Var[ZkSession],
  val statsReceiver: StatsReceiver,
  healthStabilizationEpoch: Epoch
) {
  import ServiceDiscoverer._

  private[this] val zkEntriesReadStat = statsReceiver.scope("entries").stat("read_ms")
  private[this] val zkVectorsReadStat = statsReceiver.scope("vectors").stat("read_ms")

  private[this] val actZkSession = Activity(varZkSession.map(Activity.Ok(_)))

  /**
   * Monitor the session status of the ZkSession and expose to listeners whether
   * the connection is healthy or unhealthy. Exposed for testing
   */
  private[serverset2] val rawHealth: Var[ClientHealth] = Var.async[ClientHealth](ClientHealth.Healthy) { u =>
    @volatile var stateListener = Closable.nop

    val sessionChanges = varZkSession.changes.dedup.respond { zk =>
      // When the zk session changes, we need to stop observing changes
      // to the previous session.
      synchronized {
        stateListener.close()
        stateListener = zk.state.changes.dedup.respond {
          case WatchState.SessionState(state) =>
            u() = ClientHealth(state)
          case _ => // don't need to update on non-sessionstate events
        }
      }
    }

    Closable.all(sessionChanges,
      Closable.make(t => stateListener.close(t))
    )
  }

  /**
   * Monitor the session state of the ZkSession within a HealthStabilizer
   * which only reports unhealthy when the rawHealth has been unhealthy for
   * a long enough time (as defined by the stabilization epoch).
   */
  private[serverset2] val health: Var[ClientHealth] =
    HealthStabilizer(rawHealth, healthStabilizationEpoch, statsReceiver)

  private[this] def timedOf[T](stat: Stat)(f: => Activity[T]): Activity[T] = {
    val elapsed = Stopwatch.start()
    f.map { rv =>
      stat.add(elapsed().inMilliseconds)
      rv
    }
  }

  /**
   * Activity to keep a hydrated list of Entrys or Vectors for a given ZK path.
   * protected for testing
   */
  protected[this] def entitiesOf[Entity](
    path: String,
    cache: ZkNodeDataCache[Entity],
    readStat: Stat,
    glob: String
  ): Activity[Seq[Entity]] = {
    actZkSession.flatMap { case zkSession =>
      cache.setSession(zkSession)
      zkSession.globOf(path + glob).flatMap { paths =>
        // Remove any cached entries not surfaced by globOf from our cache
        (cache.keys &~ paths).foreach(cache.remove)
        timedOf(readStat) {
          // We end up with a Seq[Seq[Entity]] here, b/c cache.get() returns a Seq[Entity]
          // flatten() to fix this (see the comment on ZkNodeDataCache for why we get a Seq[])
          Activity.future(Future.collectToTry(paths.toSeq.map(cache.get))
            .map(tries => tries.collect { case Return(e) => e }.flatten)
          )
        }
      }
    }
  }

  // protected for testing
  protected[this] val entriesOf: String => Activity[Seq[Entry]] = Memoize { path: String =>
    entitiesOf(path, new ZkEntryCache(path, statsReceiver), zkEntriesReadStat, EndpointGlob)
  }

  private[this] val vectorsOf: String => Activity[Seq[Vector]] = Memoize { path: String =>
    entitiesOf(path, new ZkVectorCache(path, statsReceiver), zkVectorsReadStat, VectorGlob)
  }

  /**
   * Look up the weighted ServerSet entries for a given path.
   */
  def apply(path: String): Activity[Seq[(Entry, Double)]] = {
    val es = entriesOf(path)
    val vs = vectorsOf(path)

    val raw = es.join(vs).map { case (ents, vecs) => zipWithWeights(ents, vecs.toSet) }

    // Squash duplicate updates
    Activity(Var(Activity.Pending, raw.states.dedup))
  }
}
