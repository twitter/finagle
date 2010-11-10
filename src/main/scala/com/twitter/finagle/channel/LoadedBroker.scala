package com.twitter.finagle.channel

import scala.util.Random
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import org.jboss.netty.channel.MessageEvent

import com.twitter.util.{Time, Duration}
import com.twitter.util.TimeConversions._
import com.twitter.finagle.util.{
  TimeWindowedSample, ScalarSample, Ok, Error, SampleLeaf, SampleNode}

import com.twitter.finagle.util.Conversions._

class TooFewDicksOnTheDanceFloorException extends Exception

/**
 * This is F-bounded to ensure that we have a homogenous set of
 * LoadedBrokers in a given load balancer. We need this so that their
 * load/weights are actually meaningfully comparable.
 */
trait LoadedBroker[A <: LoadedBroker[A]] extends Broker {
  def load: Int
  def weight: Float = 1.0f / (load.toFloat + 1.0f)
}

/**
 * Keeps track of request latencies & counts.
 */
class StatsLoadedBroker(underlying: Broker, bucketCount: Int, bucketDuration: Duration)
  extends LoadedBroker[StatsLoadedBroker]
{
  // Default: 10-minute window with 10-second buckets.
  def this(underlying: Broker) = this(underlying, 60, 10.seconds)

  private def makeStat =
    new TimeWindowedSample[ScalarSample](bucketCount, bucketDuration)

  // 5 minutes, 10 second intervals
  private val dispatchStats = makeStat
  private val latencyStats  = makeStat

  private val exceptionStats =
    new ConcurrentHashMap[String, TimeWindowedSample[ScalarSample]]

  def roots = Seq(
    SampleLeaf("count", dispatchStats),
    SampleLeaf("latency", latencyStats),
    SampleNode("failure", exceptionStats map ((SampleLeaf(_, _)).tupled) toSeq)
  )

  def dispatch(e: MessageEvent) = {
    val begin = Time.now
    dispatchStats.incr()

    underlying.dispatch(e) whenDone0 { future =>
      future {
        case Ok(_) =>
          latencyStats.add(begin.ago.inMilliseconds.toInt)
        case Error(e) =>
          // TODO: exception hierarchy here to differentiate between
          // application, connection & other (internal?) exceptions.
          val name = e.getClass.getName
          if (!(exceptionStats containsKey name))
            exceptionStats.putIfAbsent(name, makeStat)
          exceptionStats(name).add(begin.ago.inMilliseconds.toInt)
      }
    }
  }

  def load = dispatchStats.count
  // Fancy pants:
  // latencyStats.sum + 2 * latencyStats.mean * failureStats.count
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
    for ((endpoint, weight) <- snapshot) {
      val normalizedWeight = weight / totalSum
      cumulativeWeight += normalizedWeight
      if (pick < cumulativeWeight)
        return endpoint.dispatch(e)
    }

    null // Impossible
  }
}
