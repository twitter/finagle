package com.twitter.finagle

import java.net.{SocketAddress, InetSocketAddress}

/**
 * A SocketAddress with a weight. `WeightedSocketAddress` can be nested.
 */
case class WeightedSocketAddress(addr: SocketAddress, weight: Double) extends SocketAddress

object WeightedSocketAddress {
  /**
   * Extract `SocketAddress` and weight recursively until it reaches
   * an unweighted address instance. Weights are multiplied.
   *
   * If the input `addr` is an unweighted instance, return a weight of 1.0.
   */
  def extract(addr: SocketAddress): (SocketAddress, Double) =
    addr match {
      case WeightedSocketAddress(sa, weight) =>
        val (underlying, anotherW) = extract(sa)
        (underlying, weight * anotherW)
      case _ =>
        (addr, 1.0)
    }
}

object WeightedInetSocketAddress {
  /**
   * Destructuring a weighted inet socket address is liberal: we
   * return a weight of 1 if it is unweighted.
   */
  def unapply(addr: SocketAddress): Option[(InetSocketAddress, Double)] = {
    val (base, weight) = WeightedSocketAddress.extract(addr)
    base match {
      case sa: InetSocketAddress => Some(sa, weight)
      case _ => None
    }
  }
}