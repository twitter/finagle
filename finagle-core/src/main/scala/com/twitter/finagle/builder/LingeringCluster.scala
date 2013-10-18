package com.twitter.finagle.builder

import com.google.common.collect.ConcurrentHashMultiset
import com.twitter.concurrent.{Spool, SpoolSource}
import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Duration, Future, Timer}
import scala.collection.JavaConverters._

/**
 * A Cluster implementation that allows its element to linger.
 *
 * A potential use case is when underlying cluster is DNS-backed,
 * continuously refreshing addresses for a host with a low TTL
 * (e.g. Akamai) and we're behind a firewall that updates from DNS as
 * well. To maximise our chances of getting through, we want old
 * addresses to stick around for a while, along with the new ones.
 */
class LingeringCluster[T](underlying: Cluster[T], delay: Duration,
  timer: Timer = DefaultTimer.twitter)
  extends Cluster[T] {

  import Cluster._

  def snap: (Seq[T], Future[Spool[Change[T]]]) = {
    val outgoing = new SpoolSource[Change[T]]

    val (init, changes) = underlying.snap

    // the same element can be added and removed repeatedly
    val multiset = ConcurrentHashMultiset.create(init.asJava)

    for (spool <- changes; change <- spool) change match {
      case Add(node) =>
        if (multiset.add(node, 1) == 0)
          outgoing.offer(change)

      case Rem(node) =>
        timer.doLater(delay) {
          if (multiset.remove(node, 1) == 1)
              outgoing.offer(change)
        }
    }

    (init, outgoing())
  }
}
