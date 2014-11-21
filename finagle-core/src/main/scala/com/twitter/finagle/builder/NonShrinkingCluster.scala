package com.twitter.finagle.builder

import com.twitter.concurrent.Spool
import com.twitter.concurrent.SpoolSource
import com.twitter.util.Future
import java.util.ArrayDeque

/**
 * Dynamic clusters, such as serversets, can be volatile and untrustworthy. Finagle has
 * mechanisms for handling failed nodes that haven't been removed from the cluster. However
 * there are cases where a server's entry me be falsely removed from the cluster thus
 * causing clients to exert unnecessary pressure on remaining nodes. This filter reduces that
 * impact by delaying node removals. We do so by disallowing the cluster to shrink. Add
 * events are always propagated but Rem events require a corresponding Add.
 */
@deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "2014-11-21")
class NonShrinkingCluster[T](underlying: Cluster[T]) extends Cluster[T] {
  import Cluster._

  def snap: (Seq[T], Future[Spool[Change[T]]]) = {
    val outgoing = new SpoolSource[Change[T]]
    val queue = new ArrayDeque[T]

    val (init, changes) = underlying.snap

    for (spool <- changes; change <- spool) queue.synchronized {
      change match {
        case Add(node) if queue.contains(node) =>
          queue.remove(node)
        case Add(node) =>
          Option(queue.poll()) foreach { c => outgoing.offer(Rem(c)) }
          outgoing.offer(change)
        case Rem(node) =>
          queue.offer(node)
      }
    }
    (init, outgoing())
  }
}
