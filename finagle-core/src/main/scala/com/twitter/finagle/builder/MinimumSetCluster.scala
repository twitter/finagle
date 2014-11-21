package com.twitter.finagle.builder

import com.twitter.concurrent.Spool
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.Future

/**
 * A Cluster implementation that guarantees a minimum set while allowing you
 * to specify a Cluster to supplement the initial static set. All operations
 * that would remove entries in the minimum set are censored and counted.
 */
@deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "2014-11-21")
class MinimumSetCluster[T](
  minimum: Set[T],
  supplementary: Cluster[T],
  statsReceiver: StatsReceiver = NullStatsReceiver
) extends Cluster[T] {

  private[this] val censoredAdd = statsReceiver.counter("censored_add")
  private[this] val censoredRem = statsReceiver.counter("censored_rem")

  private[this] val missingGauge = statsReceiver.addGauge("missing") {
    (supplementary.snap._1 diff minimum.toSeq).size
  }

  private[this] val additionalGauge = statsReceiver.addGauge("additional") {
    (minimum.toSeq diff supplementary.snap._1).size
  }

  def snap: (Seq[T], Future[Spool[Cluster.Change[T]]]) = {
    val (supplementaryCluster, supplementaryUpdates) = supplementary.snap

    val unionCluster = (minimum ++ Set(supplementaryCluster: _*)).toSeq

    val censoredUpdates = supplementaryUpdates flatMap { updates =>
      updates filter { update =>
        val ignore = minimum.contains(update.value)
        if (ignore) {
          update match {
            case Cluster.Add(_) => censoredAdd.incr()
            case Cluster.Rem(_) => censoredRem.incr()
          }
        }

        !ignore
      }
    }

    (unionCluster, censoredUpdates)
  }
}
