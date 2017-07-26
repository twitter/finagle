package com.twitter.finagle

import com.twitter.util.{Closable, Var, FuturePool}

/**
 * A Resolver that asynchronously resolves an inet address.
 *
 * Used by ResolutionRaceTest.
 */
class TestAsyncInetResolver extends Resolver {
  val scheme: String = "asyncinet"

  private[this] object Port {
    val range = 1 to 65535
    def unapply(str: String): Option[Int] = {
      try Some(Integer.parseInt(str)) filter (range contains _)
      catch { case _: NumberFormatException => None }
    }
  }

  private[this] object HostPort {
    def unapply(str: String): Option[(String, Int)] =
      str.split(':') match {
        case Array(host, Port(port)) => Some((host, port))
        case _ => None
      }
  }

  private[this] val pool = FuturePool.unboundedPool

  def bind(spec: String): Var[Addr] = spec match {
    case HostPort(host, port) =>
      Var.async[Addr](Addr.Pending) { update =>
        pool {
          update() = Addr.Bound(Address(host, port))
        }
        Closable.nop
      }

    case _ => Var.value(Addr.Failed(spec + " is not a host:port"))
  }

}
