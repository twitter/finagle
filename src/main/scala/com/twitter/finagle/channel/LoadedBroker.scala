package com.twitter.finagle.channel

import scala.util.Random

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.channel.MessageEvent

import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.finagle.util.{TimeWindowedStatistic, ScalarStatistic}

class TooFewDicksOnTheDanceFloorException extends Exception

trait LoadedBroker[A <: LoadedBroker[A]] extends Broker {
  def load: Int
  def weight: Float = 1.0f / (load.toFloat + 1.0f)
}

/**
 * Keeps track of request latencies & counts.
 */
class StatsLoadedBroker(underlying: Broker) extends LoadedBroker[StatsLoadedBroker]
{
  // 5 minutes, 10 second intervals
  private val dispatchStats = new TimeWindowedStatistic[ScalarStatistic](60, 10.seconds)

  def dispatch(e: MessageEvent) = {
    val begin = Time.now
    dispatchStats.incr()

    underlying.dispatch(e) whenDone {
      dispatchStats.add(begin.ago.inMilliseconds.toInt, 0)
    }
  }

  def load = dispatchStats.count
}

class LeastLoadedBroker[A <: LoadedBroker[A]](endpoints: Seq[A]) extends Broker {
  implicit val ordering: Ordering[A] = Ordering.by(_.load)
  def dispatch(e: MessageEvent) = endpoints.min.dispatch(e)
}

class LoadBalancedBroker[A <: LoadedBroker[A]](endpoints: Seq[A]) extends Broker {
  val rng = new Random

  def dispatch(e: MessageEvent): ReplyFuture = {
    val snapshot = endpoints map { e => (e, e.weight) }
    val totalSum = snapshot.foldLeft(0.0f) { case (a, (_, weight)) => a + (weight) }

    // TODO: test this & other edge cases
    if (totalSum <= 0.0f)
      return ReplyFuture.failed(new TooFewDicksOnTheDanceFloorException)

    val pick = rng.nextFloat()

    var cumulativeWeight = 0.0
    for ((balancer, weight) <- snapshot) {
      val normalizedWeight = weight / totalSum
      cumulativeWeight += normalizedWeight
      if (pick < cumulativeWeight) return balancer.dispatch(e)
    }

    ReplyFuture.failed(new TooFewDicksOnTheDanceFloorException)
  }
}
