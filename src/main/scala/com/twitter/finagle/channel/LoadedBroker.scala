package com.twitter.finagle.channel

import scala.util.Random
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import org.jboss.netty.channel.MessageEvent

import com.twitter.util.{Time, Duration}
import com.twitter.util.TimeConversions._
import com.twitter.finagle.util._
import com.twitter.finagle.util.Conversions._

/**
 * This is F-bounded to ensure that we have a homogenous set of
 * LoadedBrokers in a given load balancer. We need this so that their
 * load/weights are actually meaningfully comparable.
 */
trait LoadedBroker[+A <: LoadedBroker[A]] extends Broker {
  def load: Int
  def weight: Float = if (!super.isAvailable) 0.0f else 1.0f / (load.toFloat + 1.0f)
  override def isAvailable = weight > 0.0f
}

/**
 * Keeps track of request latencies & counts.
 */
class StatsLoadedBroker(
    val underlying: Broker,
    samples: SampleRepository[T forSome { type T <: AddableSample[T] }],
    bias: Float = 1.0f)
  extends WrappingBroker
  with LoadedBroker[StatsLoadedBroker]
{
  val dispatchSample = samples("dispatch")
  val latencySample  = samples("latency")

  override def dispatch(e: MessageEvent) = {
    val begin = Time.now
    dispatchSample.incr()

    underlying.dispatch(e) whenDone0 { future =>
      future {
        case Ok(_) =>
          latencySample.add(Duration.since(begin).inMilliseconds.toInt)
        case Error(e) =>
          // TODO: exception hierarchy here to differentiate between
          // application, connection & other (internal?) exceptions.
          samples("exception", e.getClass.getName)
            .add(Duration.since(begin).inMilliseconds.toInt)
        case Cancelled => /*ignore*/ ()
      }
    }
  }

  override def weight = super.weight * bias
  def load = dispatchSample.count
  // Fancy pants:
  // latencyStats.sum + 2 * latencyStats.mean * failureStats.count
}

class FailureAccruingLoadedBroker(
    val underlying: LoadedBroker[_],
    samples: SampleRepository[TimeWindowedSample[_]])
  extends WrappingBroker
  with LoadedBroker[FailureAccruingLoadedBroker]
{
  val successSample = samples("success")
  val failureSample = samples("failure")

  def load = underlying.load

  override def weight = {
    val success = successSample.count
    val failure = failureSample.count
    val sum = success + failure

    // TODO: do we decay this decision beyond relying on the stats
    // that are passed in?

    if (sum <= 0)
      underlying.weight
    else
      (success.toFloat / (success.toFloat + failure.toFloat)) * underlying.weight
  }

  override def dispatch(e: MessageEvent) = {
    // TODO: discriminate request errors vs. connection errors, etc.?
    underlying.dispatch(e) whenDone0 { future =>
      future {
        case Ok(_)     => successSample.incr()
        case Error(_)  => failureSample.incr()
        case Cancelled => ()
      }
    }
  }
}

abstract class LoadBalancingBroker[A <: LoadedBroker[A]](endpoints: Seq[A])
  extends Broker
{
  override def isAvailable = endpoints.find(_.isAvailable).isDefined
}

class LeastLoadedBroker[A <: LoadedBroker[A]](endpoints: Seq[A])
  extends LoadBalancingBroker[A](endpoints)
{
  def dispatch(e: MessageEvent) = {
    val candidates = endpoints.filter(_.weight > 0.0f)
    if (candidates isEmpty)
      ReplyFuture.failed(new NoBrokersAvailableException)
    else
      candidates.min(Ordering.by((_: A).load)).dispatch(e)
  }
}

class LoadBalancedBroker[A <: LoadedBroker[A]](endpoints: Seq[A])
  extends LoadBalancingBroker[A](endpoints)
{
  val rng = new Random

  def dispatch(e: MessageEvent): ReplyFuture = {
    val snapshot = endpoints map { e => (e, e.weight) }
    val totalSum = snapshot map { case (_, w) => w } sum

    if (totalSum <= 0.0f)
      return ReplyFuture.failed(new NoBrokersAvailableException)

    val pick = rng.nextFloat()
    var cumulativeWeight = 0.0
    for ((endpoint, weight) <- snapshot) {
      val normalizedWeight = weight / totalSum
      cumulativeWeight += normalizedWeight
      if (pick < cumulativeWeight)
        return endpoint.dispatch(e)
    }

    // The above loop should have returned.
    ReplyFuture.failed(
      new InternalError("Impossible load balancing condition"))
  }
}
