package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{Gauge, Stat, StatsReceiver}
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import java.nio.charset.Charset
import scala.collection.concurrent.{TrieMap => ConcurrentTrieMap}

private[serverset2] object ServiceDiscoverer {

  val DefaultRetrying = 5.seconds
  val Utf8 = Charset.forName("UTF-8")
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
  private[this] val zkEntriesParseStat = statsReceiver.scope("entries").stat("parse_ms")
  private[this] val zkVectorsReadStat = statsReceiver.scope("vectors").stat("read_ms")
  private[this] val zkVectorsParseStat = statsReceiver.scope("vectors").stat("parse_ms")
  private[this] val logger = Logger(getClass)

  private[this] var gauges: Seq[Gauge] = Seq.empty

  // visible for testing.
  private[serverset2] val entriesOfCluster = Memoize { clusterPath: String =>
    val entries = new ConcurrentTrieMap[String, Seq[Entry]]
    synchronized {
      gauges = gauges :+ statsReceiver.addGauge("numberOfEntryNodes") { entries.size }
    }
    entries
  }

  private[serverset2] val vectorsOfCluster = Memoize { clusterPath: String =>
    new ConcurrentTrieMap[String, Seq[Vector]]
  }

  private[this] val actZkSession =
    // We use Var.async here to ensure that caches are shared among all
    // observers of actZkSession.
    Activity(Var.async[Activity.State[ZkSession]](Activity.Pending) { u =>
      varZkSession.changes.dedup.respond { zkSession =>
        u() = Activity.Ok(zkSession)
      }
    })

  private[this] def timedOf[T](stat: Stat)(f: => Activity[T]): Activity[T] = {
    val elapsed = Stopwatch.start()
    f map { rv =>
      stat.add(elapsed().inMilliseconds)
      rv
    }
  }

  private[this] def entriesFromEntryPath(zkSession: ZkSession, path: String) = {
    zkSession.immutableDataOf(path) map {
      case Some(Buf.Utf8(data)) =>
        val results = Entry.parseJson(path, data)
        logger.debug(s"$path retrieved ${results.length} entries")
        results
      case None => Seq()
    }
  }

  private[this] def vectorFromVectorPath(zkSession: ZkSession, path: String) = {
    zkSession.immutableDataOf(path) map {
      case Some(Buf.Utf8(data)) =>
        val results = Vector.parseJson(data)
        val vec = results.getOrElse(Vector(Nil))
        logger.debug(s"$path retrieved ${vec.vector.length} vector entries")
        Seq(vec)
      case None => Seq()
    }
  }

  /**
   * Activity to keep a hydrated list of Entrys or Vectors for a given ZK path.
   * Maintains it's own cache of entries for this path/type, and deals with
   * cache removal.
   */
  private[this] def entitiesOf[Entity](
    path: String,
    getCache: String => ConcurrentTrieMap[String, Seq[Entity]],
    glob: String,
    entitiesFromPath: (ZkSession, String) => Future[Seq[Entity]]
  ): Activity[Seq[Entity]] = {
    // This cache caches full zk path -> parsed json data for members and vectors. This assumes
    // that this data is immutable, and that new entries never re-use paths. This is true so long
    // as we (1) use ephermeral/sequential nodes for instances, and (2) the parent nodes of these
    // members/vectors are not deleted and recreated.
    val cache = getCache(path)
    actZkSession flatMap { case zkSession =>
      zkSession.globOf(path + glob).flatMap { paths =>
        timedOf(zkEntriesReadStat) {
          Activity.future(
            // Fetch data for any nodes (member_ or vector_ paths) surfaced by globOf
            // that were not already cached
            Future.collectToTry( (paths &~ cache.keys.toSet).toSeq.map { pathToAdd =>
                entitiesFromPath(zkSession, pathToAdd) map { entities =>
                  (pathToAdd, entities)
                }
              }
            ).map(tries => tries.collect { case Return(e) => e }).map { entitiesToAdd =>
              // Add new entries to cache
              cache ++= entitiesToAdd
              // Remove any cached entries not surfaced by globOf from our cache
              cache --= (cache.keys.toSet &~ paths)
              cache.values.flatten.toSeq
            }
          )
        }
      }
    }
  }

  private[this] val entriesOf = Memoize { path: String =>
    entitiesOf(path, entriesOfCluster, EndpointGlob, entriesFromEntryPath)
  }

  private[this] val vectorsOf = Memoize { path: String =>
    entitiesOf(path, vectorsOfCluster, VectorGlob, vectorFromVectorPath)
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
