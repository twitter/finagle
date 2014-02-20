package com.twitter.finagle

import java.net.SocketAddress

/**
 * A SocketAddress with a weight.
 */
case class WeightedSocketAddress(
  addr: SocketAddress,
  weight: Double
) extends SocketAddress
