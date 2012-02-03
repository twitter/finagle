package com.twitter.finagle.loadbalancer

import annotation.tailrec
import collection.mutable.ArrayBuffer
import util.Random

import com.twitter.util.{Future, Return, Time, Try}

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.NoBrokersAvailableException
import com.twitter.finagle.stats.StatsReceiver

/**
 * A LoadBalancerStrategy produces a sequence of factories and weights.
 */
trait LoadBalancerStrategy {
  def apply[Req, Rep](
    factories: Seq[ServiceFactory[Req, Rep]]
  ): Seq[(ServiceFactory[Req, Rep], Float)]
}

/**
 * Returns a service by applying a Seq of LoadBalancerStrategies to a
 * Seq of ServiceFactories, multiplying the weights for each factory,
 * and choosing a factory of the highest weight.
 */
class LoadBalancedFactory[Req, Rep](
    factories: Seq[ServiceFactory[Req, Rep]],
    statsReceiver: StatsReceiver,
    strategies: LoadBalancerStrategy*)
  extends ServiceFactory[Req, Rep]
{
  // initialize using Time.now for predictable test behavior
  private[this] val rng = new Random(Time.now.inMillis)

  // TODO: make this work for dynamic Seq[Factory..]
  private[this] val gauges = {
    factories map { factory =>
      statsReceiver.scope("available").addGauge(factory.toString) {
        if (factory.isAvailable) 1F else 0F
      }

      statsReceiver.scope("weight").addGauge(factory.toString) {
        weight(factory)
      }
    }
  }

  def make(): Future[Service[Req, Rep]] = {
    val available = availableOrAll
    if (available.isEmpty)
      return Future.exception(new NoBrokersAvailableException)

    max(weights(available)).make()
  }

  // Compute the weight of the given factory. Used for stats reporting
  // only. This is also used in testing.
  private[finagle] def weight(factory: ServiceFactory[Req, Rep]): Float = {
    var snapshot = factories.toSeq
    if (!(snapshot contains factory))
      snapshot ++= Seq(factory)
    weights(snapshot) find { case (f, _) => f equals factory } match {
      case Some((_, weight)) => weight
      case None => 0F
    }
  }

  private[this] def weights(
    available: Seq[ServiceFactory[Req, Rep]]
  ): Seq[(ServiceFactory[Req, Rep], Float)] = {
    val base = available map((_, 1F))
    applyWeights(base, strategies.toList)
  }

  private[this] def availableOrAll: Seq[ServiceFactory[Req, Rep]] = {
    // We first create a snapshot since the underlying seq could
    // change out from under us
    val snapshot = factories.toSeq

    val available = snapshot filter { _.isAvailable }

    // If none are available, we load balance over all of them. This
    // is to remedy situations where the health checking becomes too
    // pessimistic.
    if (available.isEmpty)
      snapshot
    else
      available
  }

  @tailrec
  private[this] def applyWeights(
    weightedFactories: Seq[(ServiceFactory[Req, Rep], Float)],
    strategies: List[LoadBalancerStrategy]
  ): Seq[(ServiceFactory[Req, Rep], Float)] = strategies match {
    case Nil =>
      weightedFactories
    case strategy :: rest =>
      val (factories, weights) = weightedFactories unzip
      val (newFactories, newWeights) = strategy(factories) unzip
      val combinedWeights = (weights zip newWeights) map (Function.tupled { _ * _ })
      applyWeights(newFactories zip combinedWeights, rest)
  }

  private[this] def max(
    weightedFactories: Seq[(ServiceFactory[Req, Rep], Float)]
  ): ServiceFactory[Req, Rep] = {
    var maxWeight = Float.MinValue
    val maxes = new ArrayBuffer[ServiceFactory[Req, Rep]]

    weightedFactories foreach { case (factory, weight) =>
      if (weight > maxWeight) {
        maxes.clear()
        maxes += factory
        maxWeight = weight
      } else if (weight == maxWeight) {
        maxes += factory
      }
    }

    val index = if (maxes.size == 1) 0 else rng.nextInt(maxes.size)
    maxes(index)
  }

  override def isAvailable = factories.exists(_.isAvailable)

  override def close() = factories foreach { _.close() }

  override val toString = "load_balanced_factory_%s".format(
    factories map { _.toString } mkString(","))
}
