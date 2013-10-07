package com.twitter.finagle

import collection.immutable
import java.net.SocketAddress
import com.twitter.util.Var

/**
 * A name identifies an object. Names may be resolved from strings
 * via [[com.twitter.finagle.Resolver]]s. Names are late bound: their
 * addresses must be recovered by calling `bind`.
 */
trait Name {
  /**
   * Bind the name. The bound name is returned as a 
   * variable representation -- it is subject to change at
   * any time.
   */
  def bind(): Var[Addr]
}

object Name {
  /**
   * Create a pre-bound address.
   */
  def bound(addrs: SocketAddress*): Name = new Name {
    def bind() = Var.value(Addr.Bound(addrs:_*))
  }
  
  /**
   * Create a name from a group.
   */
  def fromGroup(g: Group[SocketAddress]): Name = new Name {
    def bind() = g.set map { newSet => Addr.Bound(newSet) }
  }
}
