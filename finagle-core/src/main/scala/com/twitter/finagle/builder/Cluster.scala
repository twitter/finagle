package com.twitter.finagle.builder

import com.twitter.concurrent.Spool
import com.twitter.util.Future
import collection.mutable.HashMap

/**
 * Cluster is a collection of servers. The intention of this interface
 * to express membership in a cluster of servers that provide a
 * specific service.
 *
 * Note that a Cluster can be elastic: members can join or leave at
 * any time.
 */
trait Cluster[T] { self =>
  /**
   * Takes a snapshot of the collection; returns the current elements and a Spool of future updates
   */
  def snap: (Seq[T], Future[Spool[Cluster.Change[T]]])

  /**
   * Maps the elements as well as future updates to the given type.
   */
  def map[U](f: T => U): Cluster[U] = new Cluster[U] {
    // Translation cache to ensure that mapping is idempotent.
    private[this] val cache = HashMap.empty[T, U]
    private[this] def g(t: T) = synchronized { cache.getOrElseUpdate(t, f(t)) }

    def snap: (Seq[U], Future[Spool[Cluster.Change[U]]]) = {
      val (seqT, changeT) = self.snap
      val seqU = seqT map g
      val changeU = changeT map { _ map {
          case Cluster.Add(t) => Cluster.Add(g(t))
          case Cluster.Rem(t) => Cluster.Rem(g(t))
        }
      }
      (seqU, changeU)
    }
  }
}

object Cluster {
  sealed abstract trait Change[T]
  case class Add[T](t: T) extends Change[T]
  case class Rem[T](t: T) extends Change[T]
}

/**
 * A simple static cluster implementation.
 */
class StaticCluster[T](underlying: Seq[T]) extends Cluster[T] {
  def snap: (Seq[T], Future[Spool[Cluster.Change[T]]]) = (underlying, Future.value(Spool.empty))
}
