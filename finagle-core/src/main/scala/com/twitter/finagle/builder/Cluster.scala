package com.twitter.finagle.builder

import com.twitter.concurrent.Spool
import com.twitter.util.Future
import collection.mutable
import java.util.logging.Logger

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
    private[this] val mapped = mutable.HashMap.empty[T, mutable.Queue[U]]

    def snap: (Seq[U], Future[Spool[Cluster.Change[U]]]) = {
      val (seqT, changeT) = self.snap
      val seqU = seqT map { t =>
        val q = mapped.getOrElseUpdate(t, mutable.Queue[U]())
        val u = f(t)
        q.enqueue(u)
        u
      }

      val changeU = changeT map { spoolT =>
        spoolT map { elem =>
          mapped.synchronized {
            elem match {
              case Cluster.Add(t) =>
                val q = mapped.getOrElseUpdate(t, mutable.Queue[U]())
                val u = f(t)
                q.enqueue(u)
                Cluster.Add(u)

              case Cluster.Rem(t) =>
                 mapped.get(t) match {
                   case Some(q) =>
                     val u = q.dequeue()
                     if (q.isEmpty)
                       mapped.remove(t)
                     Cluster.Rem(u)

                   case None =>
                     Logger.getLogger("").warning(
                       "cluster does not have removed key, regenerating")
                     Cluster.Rem(f(t))
                 }
            }
          }
        }
      }

      (seqU, changeU)
    }
  }
}

object Cluster {
  sealed abstract trait Change[T] { def value: T }
  case class Add[T](value: T) extends Change[T]
  case class Rem[T](value: T) extends Change[T]
}

/**
 * A simple static cluster implementation.
 */
class StaticCluster[T](underlying: Seq[T]) extends Cluster[T] {
  def snap: (Seq[T], Future[Spool[Cluster.Change[T]]]) = (underlying, Future.value(Spool.empty))
}
