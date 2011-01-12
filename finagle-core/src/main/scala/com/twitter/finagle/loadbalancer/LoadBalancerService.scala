package com.twitter.finagle.loadbalancer

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.{Future, MapMaker, Time, Duration, Return, Throw}
import com.twitter.conversions.time._

import com.twitter.finagle.service.Service
import com.twitter.finagle.channel.NoBrokersAvailableException
import com.twitter.finagle.stats.{ReadableCounter, TimeWindowedStatsRepository}

class ServiceMeta[T](default: => T) extends Iterable[(Service[_, _], T)]  {
  private[this] val serviceToMeta =
    MapMaker[Service[_, _], T] { config => config.weakKeys }

  def apply(service: Service[_, _]) =
    serviceToMeta.getOrElseUpdate(service, default)

  def iterator = serviceToMeta.iterator
}

object ServiceMeta {
  def apply[T](default: => T) = new ServiceMeta[T](default)
}

trait LoadBalancerStrategy[Req <: AnyRef, Rep <: AnyRef] {
  def dispatch(
    request: Req,
    services: Seq[Service[Req, Rep]]): Option[(Service[Req, Rep], Future[Rep])]
}

class FailureAccrualStrategy[Req, Rep](
  underlying: LoadBalancerStrategy[Req, Rep],
  numFailures: Int,
  markDeadFor: Duration)
  extends LoadBalancerStrategy[Req, Rep]
{
  private class FailureMeta {
    private[this] var failureCount = 0
    private[this] var failedAt = Time.epoch

    def isDead = synchronized { failedAt.untilNow < markDeadFor }

    def didFail() = synchronized {
      failureCount += 1
      if (failureCount > numFailures)
        failedAt = Time.now
    }

    def didSucceed() = synchronized {
      failureCount = 0
      failedAt = Time.epoch
    }
  }

  private[this] val meta = ServiceMeta[FailureMeta] { new FailureMeta }

  private[this] val failureCount =
    new ServiceMeta[AtomicInteger](new AtomicInteger(0))
  private[this] val failedAt = new ServiceMeta[Time](Time.epoch)

  // If all nodes are marked bad--mark none of them bad?

  def dispatch(request: Req, services: Seq[Service[Req, Rep]]) = {
    val filtered = services filter { service => !meta(service).isDead }
    val result = underlying.dispatch(request, filtered)
    result foreach { case (service, resultFuture) =>
      resultFuture respond {
        case Throw(_)  => meta(service).didFail()
        case Return(_) => meta(service).didSucceed()
      }
    }

    result
  }
}

class LeastLoadedStrategy[Req <: AnyRef, Rep <: AnyRef]
  extends LoadBalancerStrategy[Req, Rep]
{
  private[this] val loadStat = ServiceMeta[ReadableCounter] {
    (new TimeWindowedStatsRepository(10, 1.seconds)).counter()
  }

  // TODO: account for recently introduced services.
  val leastLoadedOrdering = new Ordering[Service[Req, Rep]] {
    def compare(a: Service[Req, Rep], b: Service[Req, Rep]) =
      loadStat(a).sum - loadStat(b).sum
  }

  def dispatch(request: Req, services: Seq[Service[Req, Rep]]) =
    if (services.isEmpty) {
      None
    } else {
      val selected = services.min(leastLoadedOrdering)
      loadStat(selected).incr()
      Some((selected, selected(request)))
    }
}

class LoadBalancerService[-Req <: AnyRef, +Rep <: AnyRef](
  services: Seq[Service[Req, Rep]],
  strategy: LoadBalancerStrategy[Req, Rep])
  extends Service[Req, Rep]
{
  def apply(request: Req) = {
    strategy.dispatch(request, services) match {
      case Some((_, future)) => future
      case None => Future.exception(new NoBrokersAvailableException)
    }
  }
}
