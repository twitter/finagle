package com.twitter.finagle.loadbalancer

import com.twitter.finagle.Stack

// maxEffort is the fixed number of retries an LB implementation is willing
// to make if the distributor's pick returns an unavailable (Status.Busy or
// Status.Closed) node
sealed abstract class PanicMode private[loadbalancer] {
  def maxEffort: Int

  override def toString: String = maxEffort match {
    case 0 => "Always panic (for testing). PanicMode(0)"
    case 1 => "TenPercentUnhealthy"
    case 2 => "ThirtyPercentUnhealthy"
    case 3 => "FortyPercentUnhealthy"
    case 4 => "FiftyPercentUnhealthy"
    case 5 => "MajorityUnhealthy"
    case _ => s"Invalid value: PanicMode($maxEffort)"
  }
}

/**
 * Panic mode is when the LB gives up trying to find a healthy node. The LB
 * sends the request to the last pick even if the node is unhealthy. For a
 * given request, panic mode is enabled when the percent of nodes that are
 * unhealthy exceeds the panic threshold. This percent is approximate. For
 * pick2-based load balancers (P2C* and Aperture*), interpret this as
 * 1% of requests or less will panic when the threshold is reached. When the
 * percent of unhealthy nodes exceeds the threshold, the number of requests
 * that panic increases exponentially. For round robin, this panic threshold
 * percent does not apply because it is not a pick two based algorithm. Panic
 * mode is disabled for heap LB.
 *
 * Please, note that this doesn't mean that 1% of requests will fail since
 * Finagle clients have additional layers of requeues above the load balancer.
 */
object PanicMode {
  private[loadbalancer] final class StaticPanicMode(val maxEffort: Int) extends PanicMode

  /**
   * Example: If the proportion of unhealthy nodes is 0.5, then the
   * probability of picking two unhealthy nodes with P2C is 0.5*0.5 = 25%. How
   * many times do we repeat P2C until the probability is ε (0.01 or 1%)?
   *
   * Answer: 3.3 times because 0.25^3.3 = 0.01. Round up to maxEffort=4. When
   * 50% is unhealthy, there's a 1% chance the LB picks all unhealthy nodes
   * 4 times in a row. This means 1% of requests panic.
   */

  // Always panic immediately. For tests only
  val Paranoid = new StaticPanicMode(0)
  // 10% unhealthy, Prob(2 unhealthy) = 0.1*0.1, maxEffort=1 because 0.01^1 < ε
  val TenPercentUnhealthy = new StaticPanicMode(1)

  // 20% unhealthy is the same as 30% unhealthy.
  // Prob(2 unhealthy) = 0.2*0.2, maxEffort=2 because 0.04^2 < ε

  // 30% unhealthy, Prob(2 unhealthy) = 0.3*0.3, maxEffort=2 because 0.09^2 < ε
  val ThirtyPercentUnhealthy = new StaticPanicMode(2)
  // 40% unhealthy, Prob(2 unhealthy) = 0.4*0.4, maxEffort=3 because 0.16^3 < ε
  val FortyPercentUnhealthy = new StaticPanicMode(3)
  // 50% unhealthy, Prob(2 unhealthy) = 0.5*0.5, maxEffort=4 because 0.25^4 < ε
  val FiftyPercentUnhealthy = new StaticPanicMode(4)
  // Greater than 50% unhealthy
  val MajorityUnhealthy = new StaticPanicMode(5)

  // The default is maxEffort=4
  implicit val param: Stack.Param[PanicMode] =
    Stack.Param(PanicMode.FiftyPercentUnhealthy)
}
