package com.twitter.finagle

import java.net.{SocketAddress, InetSocketAddress}

/**
 * A SocketAddress with a weight.
 */
object WeightedSocketAddress {
  private case class Impl(
    addr: SocketAddress,
    weight: Double
  ) extends SocketAddress

  /**
   * Create a weighted socket address with weight `weight`.
   */
  def apply(addr: SocketAddress, weight: Double): SocketAddress =
    Impl(addr, weight)

  /**
   * Destructuring a weighted socket address is liberal: we return a
   * weight of 1 if it is unweighted.
   */
  def unapply(addr: SocketAddress): Option[(SocketAddress, Double)] =
    addr match {
      case Impl(addr, weight) => Some(addr, weight)
      case addr => Some(addr, 1D)
    }
}

object WeightedInetSocketAddress {
 /**
   * Destructuring a weighted inet socket address is liberal: we
   * return a weight of 1 if it is unweighted.
   */
  def unapply(addr: SocketAddress): Option[(InetSocketAddress, Double)] =
    addr match {
      case WeightedSocketAddress(ia: InetSocketAddress, weight) => Some(ia, weight)
      case ia: InetSocketAddress => Some(ia, 1D)
      case _ => None
    }
}
