package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{Stat, StatsReceiver}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference

private[serverset2] object ServiceDiscoverer {
  val DefaultRetrying = 5.seconds
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
}

/**
 * A representation of a session to a given ZooKeeper-backed service
 * discovery cluster.
 *
 * Given a ServerSet path, [[apply]] looks up the set and returns a
 * dynamic set of (entry, weight) tuples.
 */
private[serverset2] class ServiceDiscoverer(
  varZkSession: Var[ZkSession],
  statsReceiver: StatsReceiver
) {
  import ServiceDiscoverer._

  private[this] val zkEntriesReadStat = statsReceiver.scope("entries").stat("read_ms")
  private[this] val zkVectorsReadStat = statsReceiver.scope("vectors").stat("read_ms")

  private[this] val zkSession = new AtomicReference(ZkSession.nil)
  varZkSession.changes.register(Witness(zkSession))

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
    zkSession.get.globOf(path + glob).flatMap { paths =>
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

  // protected for testing
  protected[this] val entriesOf: String => Activity[Seq[Entry]] = Memoize { path: String =>
    entitiesOf(path, new ZkEntryCache({ () => zkSession.get }, path, statsReceiver), zkEntriesReadStat, EndpointGlob)
  }

  private[this] val vectorsOf: String => Activity[Seq[Vector]] = Memoize { path: String =>
    entitiesOf(path, new ZkVectorCache({ () => zkSession.get }, path, statsReceiver), zkVectorsReadStat, VectorGlob)
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
