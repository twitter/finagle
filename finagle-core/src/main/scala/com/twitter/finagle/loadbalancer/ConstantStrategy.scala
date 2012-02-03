package com.twitter.finagle.loadbalancer

import com.twitter.util.{Return, Throw, Try}

import com.twitter.finagle.{Service, ServiceFactory}

/**
 * Returns weights based on sequence of factory/weight pairs passed in
 * on instantiation.  Will throw on apply if one of the factories in
 * the sequence isn't in the initial sequence of pairs.
 *
 * A ConstantStrategy should always be at the beginning of a
 * LoadBalancerStrategy sequence unless the assignWeight method is
 * clever enough to figure out the underlying factories that preceeded
 * strategies have wrapped in their apply methods.
 */
class ConstantStrategy(
    assignWeight: (ServiceFactory[_, _] => Float))
  extends LoadBalancerStrategy
{
  override def apply[Req, Rep](
    factories: Seq[ServiceFactory[Req, Rep]]
  ): Seq[(ServiceFactory[Req, Rep], Float)] = {
    factories map { factory =>
      (factory, assignWeight(factory))
    }
  }
}

class DefaultWeightAssigner(
    weightedFactories: Seq[(ServiceFactory[_, _], Float)],
    default: Float)
  extends (ServiceFactory[_, _] => Float)
{
  val weightMap = weightedFactories.toMap
  override def apply(factory: ServiceFactory[_, _]) = weightMap.getOrElse(factory, default)
}
