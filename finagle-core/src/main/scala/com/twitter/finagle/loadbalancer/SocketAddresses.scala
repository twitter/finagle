package com.twitter.finagle.loadbalancer

import com.twitter.finagle.WeightedSocketAddress
import java.net.SocketAddress

private[loadbalancer] object SocketAddresses {
  /**
   * @param i is just to make each replica unique in a set
   */
  private case class ReplicatedSocketAddress(addr: SocketAddress, i: Int)
    extends SocketAddress

  /**
   * generates `num` number of socket addresses
   *
   * @param num the number of copies to create
   */
  def replicate(num: Int): SocketAddress => Set[SocketAddress] = {
    case sa: ReplicatedSocketAddress =>
      Set(sa)
    case sa =>
      val (base, w) = WeightedSocketAddress.extract(sa)
      for (i: Int <- (0 until num).toSet) yield
        WeightedSocketAddress(ReplicatedSocketAddress(base, i), w)
  }

  /**
   * unwrap `WeightedSocketAddress` to the underlying socket address.
   */
  def unwrap(addr: SocketAddress): SocketAddress = {
    addr match {
      case ReplicatedSocketAddress(s, _) => unwrap(s)
      case WeightedSocketAddress(s, _) => unwrap(s)
      case _ => addr
    }
  }
}