package com.twitter.finagle.builder

import collection.mutable
import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.*::
import com.twitter.util.Future
import java.util.logging.Logger

/**
 * Cluster is a collection of servers. The intention of this interface
 * to express membership in a cluster of servers that provide a
 * specific service.
 *
 * Note that a Cluster can be elastic: members can join or leave at
 * any time.
 */
@deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "2014-11-21")
trait Cluster[T] { self =>
  /**
   * A Future object that is defined when the cluster is initialized.
   * Cluster users can subscribe to this Future object to check or get notified of the availability of the cluster.
   * @return the Future object
   */
  def ready: Future[Unit] = {
    def flatten(spool: Spool[Cluster.Change[T]]): Future[Unit] = spool match {
      case Cluster.Add(_) *:: tail => Future.Done
      case _ *:: tail => tail.flatMap(flatten)
    }

    snap match {
      case (current, changes) if current.isEmpty => changes.flatMap(flatten)
      case _ => Future.Done
    }
  }

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

    override def ready = self.ready

    def snap: (Seq[U], Future[Spool[Cluster.Change[U]]]) = {
      val (seqT, changeT) = self.snap
      val seqU = mapped.synchronized {
        seqT map { t =>
          val q = mapped.getOrElseUpdate(t, mutable.Queue[U]())
          val u = f(t)
          q.enqueue(u)
          u
        }
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
@deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "2014-11-21")
case class StaticCluster[T](underlying: Seq[T]) extends Cluster[T] {
  def snap: (Seq[T], Future[Spool[Cluster.Change[T]]]) = (underlying, Future.value(Spool.empty))
}
