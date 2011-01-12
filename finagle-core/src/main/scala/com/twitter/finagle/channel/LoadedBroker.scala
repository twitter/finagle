package com.twitter.finagle.channel

import scala.util.Random
import scala.collection.JavaConversions._

import com.twitter.util.{Time, Return, Throw, Future}
import com.twitter.util.TimeConversions._
import com.twitter.finagle.util._
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.stats.StatsRepository

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
    protected val underlying: Broker,
    statsRepository: StatsRepository,
    bias: Float = 1.0f)
  extends WrappingBroker
  with LoadedBroker[StatsLoadedBroker]
{
  private[this] val dispatchStat = statsRepository.counter("name" -> "dispatches")
  private[this] val latencyStat  = statsRepository.gauge("name" -> "latency")

  override def apply(request: AnyRef) = {
    val begin = Time.now
    dispatchStat.incr()

    val f = underlying(request)

    f respond {
      case Return(_) =>
        latencyStat.measure(begin.untilNow.inMilliseconds.toInt)
      case Throw(e) =>
        // TODO: exception hierarchy here to differentiate between
        // application, connection & other (internal?) exceptions.
        statsRepository.counter("exception" -> e.getClass.getName).incr()
    }

    f
  }

  override def weight = super.weight * bias
  def load = dispatchStat.sum
  // Fancy pants:
  // latencyStats.sum + 2 * latencyStats.mean * failureStats.count
}

class FailureAccruingLoadedBroker(
    protected val underlying: LoadedBroker[_],
    statsRepository: StatsRepository)
  extends WrappingBroker
  with LoadedBroker[FailureAccruingLoadedBroker]
{
  private[this] val successStat = statsRepository.counter("name" -> "success")
  private[this] val failureStat = statsRepository.counter("name" -> "failure")

  def load = underlying.load

  override def weight = {
    val success = successStat.sum
    val failure = failureStat.sum
    val sum = success + failure

    // TODO: do we decay this decision beyond relying on the stats
    // that are passed in?

    if (sum <= 0)
      underlying.weight
    else
      (success.toFloat / (success.toFloat + failure.toFloat)) * underlying.weight
  }

  override def apply(request: AnyRef) = {
    // TODO: discriminate request errors vs. connection errors, etc.?
    val f = underlying(request)
    f respond {
      case Return(_) => successStat.incr()
      case Throw(_) => failureStat.incr()
    }

    f
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
  def apply(request: AnyRef) = {
    val candidates = endpoints.filter(_.weight > 0.0f)
    if (candidates isEmpty)
      Future.exception(new NoBrokersAvailableException)
    else
      candidates.min(Ordering.by((_: A).load))(request)
  }
}

class LoadBalancedBroker[A <: LoadedBroker[A]](endpoints: Seq[A])
  extends LoadBalancingBroker[A](endpoints)
{
  val rng = new Random

  def apply(request: AnyRef): Future[AnyRef] = {
    val snapshot = endpoints map { e => (e, e.weight) }
    val totalSum = snapshot map { case (_, w) => w } sum

    if (totalSum <= 0.0f)
      return Future.exception(new NoBrokersAvailableException)

    val pick = rng.nextFloat()
    var cumulativeWeight = 0.0
    for ((endpoint, weight) <- snapshot) {
      val normalizedWeight = weight / totalSum
      cumulativeWeight += normalizedWeight
      if (pick < cumulativeWeight)
        return endpoint(request)
    }

    // The above loop should have returned.
    Future.exception(
      new InternalError("Impossible load balancing condition"))
  }
}
