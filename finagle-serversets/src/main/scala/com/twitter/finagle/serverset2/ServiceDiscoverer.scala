package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{Stat, StatsReceiver}
import com.twitter.io.Buf
import com.twitter.util.{Activity, Memoize, Stopwatch, Var}
import java.nio.charset.Charset

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
  def zipWithWeights(ents: Seq[Entry], vecs: Set[Vector]): Seq[(Entry, Double)] =
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

  def entriesOfNode(zkSession: ZkSession) =
    Memoize { path: String =>
      Activity.future(
        zkSession.immutableDataOf(path) map {
          case Some(Buf.Utf8(data)) => Entry.parseJson(path, data)
          case None => Seq()
        })
    }

  def vectorOfNode(zkSession: ZkSession) =
    Memoize { path: String =>
      Activity.future(
        zkSession.immutableDataOf(path) map {
          case Some(Buf.Utf8(data)) => Vector.parseJson(data)
          case None => None
        })
    }

  private[this] val actZkSession =
    Activity(varZkSession.map { zkSession =>
      Activity.Ok(zkSession, entriesOfNode(zkSession), vectorOfNode(zkSession))
    })

  private[this] def timedOf[T](stat: Stat)(f: => Activity[T]): Activity[T] = {
    val elapsed = Stopwatch.start()
    f map { rv =>
      stat.add(elapsed().inMilliseconds)
      rv
    }
  }

  private[this] def entriesOf(path: String): Activity[Seq[Entry]] =
    actZkSession flatMap { case (zkSession, entriesOfNode, _) =>
      zkSession.globOf(path + EndpointGlob) flatMap { paths =>
        timedOf(zkEntriesReadStat) {
          Activity.collect(paths.map(entriesOfNode)).map(_.flatten)
        }
      }
    }

  private[this] def vectorsOf(path: String): Activity[Set[Vector]] =
    actZkSession flatMap { case (zkSession, _, vectorOfNode) =>
      zkSession.globOf(path + VectorGlob) flatMap { paths =>
        timedOf(zkVectorsReadStat) {
          Activity.collect(paths.map(vectorOfNode)).map(_.flatten.toSet)
        }
      }
    }

  /**
   * Look up the weighted ServerSet entries for a given path.
   */
  def apply(path: String): Activity[Seq[(Entry, Double)]] = {
    val es = entriesOf(path).run
    val vs = vectorsOf(path).run

    val raw = (es join vs) map {
      case (Activity.Pending, _) => Activity.Pending
      case (f@Activity.Failed(_), _) => f
      case (Activity.Ok(ents), Activity.Ok(vecs)) =>
        Activity.Ok(ServiceDiscoverer.zipWithWeights(ents, vecs))
      case (Activity.Ok(ents), _) =>
        Activity.Ok(ents map (_ -> 1D))
    }

    // Squash duplicate updates
    val dedupe = raw.changes.sliding(2) collect {
      case Seq(current) => current
      case Seq(last, next) if last != next => next
    }

    Activity(Var(Activity.Pending, dedupe))
  }
}
