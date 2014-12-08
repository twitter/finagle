package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{Stat, StatsReceiver}
import com.twitter.io.Buf
import com.twitter.util.{Activity, Stopwatch, Var}
import java.nio.charset.Charset
import com.google.common.cache.{Cache, CacheBuilder}

private[serverset2] object ServiceDiscoverer {
  class PathCache(maxSize: Int) {
    val entries: Cache[String, Set[Entry]] = CacheBuilder.newBuilder()
      .maximumSize(maxSize)
      .build()

    val vectors: Cache[String, Option[Vector]] = CacheBuilder.newBuilder()
      .maximumSize(maxSize)
      .build()
  }

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
  def zipWithWeights(ents: Set[Entry], vecs: Set[Vector]): Set[(Entry, Double)] =
    ents map { ent =>
      val w = vecs.foldLeft(1.0) { case (w, vec) => w*vec.weightOf(ent) }
      ent -> w
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

  private[this] val actZkSession = Activity(varZkSession.map(Activity.Ok(_)))

  private[this] def timedOf[T](stat: Stat)(f: => Activity[T]): Activity[T] = {
    val elapsed = Stopwatch.start()
    f map { rv =>
      stat.add(elapsed().inMilliseconds)
      rv
    }
  }

  private[this] def dataOf(
    pattern: String,
    readStat: Stat
  ): Activity[Seq[(String, Option[Buf])]] = actZkSession flatMap { zkSession =>
    zkSession.globOf(pattern) flatMap { paths =>
      timedOf(readStat)(zkSession.collectImmutableDataOf(paths))
    }
  }

  private[this] def entriesOf(
    path: String,
    cache: PathCache
  ): Activity[Set[Entry]] = {
    dataOf(path + EndpointGlob, zkEntriesReadStat) flatMap { pathmap =>
      timedOf[Set[Entry]](zkEntriesParseStat) {
        val endpoints = pathmap flatMap {
          case (_, null) => None // no data
          case (path, Some(Buf.Utf8(data))) =>
            cache.entries.getIfPresent(path) match {
              case null =>
                val ents = Entry.parseJson(path, data)
                val entset = ents.toSet
                cache.entries.put(path, entset)
                entset
              case ent => ent
            }

          case _ => None  // Invalid encoding
        }
        Activity.value(endpoints.toSet)
      }
    }
  }

  private[this] def vectorsOf(
    path: String,
    cache: PathCache
  ): Activity[Set[Vector]] = {
    dataOf(path + VectorGlob, zkVectorsReadStat) flatMap { pathmap =>
      timedOf[Set[Vector]](zkVectorsParseStat) {
        val vectors = pathmap flatMap {
          case (path, None) =>
            cache.vectors.getIfPresent(path) match {
              case null => None
              case vec => vec
            }
          case (path, Some(Buf.Utf8(data))) =>
            cache.vectors.getIfPresent(path) match {
              case null =>
                val vec = Vector.parseJson(data)
                cache.vectors.put(path, vec)
                vec
              case vec => vec
            }
          case _ => None // Invalid encoding
        }
        Activity.value(vectors.toSet)
      }
    }
  }

  /**
   * Look up the weighted ServerSet entries for a given path.
   */
  def apply(path: String): Activity[Set[(Entry, Double)]] = {
    val cache = new PathCache(16000)
    val es = entriesOf(path, cache).run
    val vs = vectorsOf(path, cache).run

    Activity((es join vs) map {
      case (Activity.Pending, _) => Activity.Pending
      case (f@Activity.Failed(_), _) => f
      case (Activity.Ok(ents), Activity.Ok(vecs)) =>
        Activity.Ok(ServiceDiscoverer.zipWithWeights(ents, vecs))
      case (Activity.Ok(ents), _) =>
        Activity.Ok(ents map (_ -> 1D))
    })
  }
}
