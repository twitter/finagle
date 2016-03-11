package com.twitter.finagle

import java.net.InetSocketAddress
import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import scala.util.control.NoStackTrace

/**
 * An [[Address]] represents the physical location of a single host or
 * endpoint. It also includes [[Addr.Metadata]] (typically set by [[Namer]]s
 * and [[Resolver]]s) that provides additional configuration to client stacks.
 *
 * Note that a bound [[Addr]] contains a set of [[Address]]es and [[Addr.Metadata]]
 * that pertains to the entire set.
 */
sealed trait Address

object Address {
  private[finagle] val failing: Address =
    Address.Failed(new IllegalArgumentException("failing") with NoStackTrace)

  /**
   * An address represented by an Internet socket address.
   */
  case class Inet(
      addr: InetSocketAddress,
      metadata: Addr.Metadata)
    extends Address

  /**
   * An address that fails with the given `cause`.
   */
  case class Failed(cause: Throwable) extends Address

  /** Create a new [[Address]] with given [[java.net.InetSocketAddress]]. */
  def apply(addr: InetSocketAddress): Address =
    Address.Inet(addr, Addr.Metadata.empty)

  /** Create a new [[Address]] with given `host` and `port`. */
  def apply(host: String, port: Int): Address =
    Address(new InetSocketAddress(host, port))

  /** Create a new loopback [[Address]] with the given `port`. */
  def apply(port: Int): Address =
    Address(new InetSocketAddress(port))
}

package exp {
  object Address {
    /** Create a new [[Address]] with the given [[com.twitter.finagle.ServiceFactory]]. */
    def apply[Req, Rep](factory: com.twitter.finagle.ServiceFactory[Req, Rep]): Address =
      Address.ServiceFactory(factory, Addr.Metadata.empty)

    /**
     * An endpoint address represented by a [[com.twitter.finagle.ServiceFactory]]
     * that implements the endpoint.
     */
    case class ServiceFactory[Req, Rep](
        factory: com.twitter.finagle.ServiceFactory[Req, Rep],
        metadata: Addr.Metadata)
      extends Address
  }
}

/**
 * A Java adaptation of the [[com.twitter.finagle.Address]] companion object.
 */
object Addresses {
  /**
   * @see com.twitter.finagle.Address.Inet
   */
  def newInetAddress(ia: InetSocketAddress): Address =
    Address.Inet(ia, Addr.Metadata.empty)

  /**
   * @see com.twitter.finagle.Address.Inet
   */
  def newInetAddress(ia: InetSocketAddress, metadata: JMap[String, Any]): Address =
    Address.Inet(ia, metadata.asScala.toMap)

  /**
   * @see com.twitter.finagle.Address.Failed
   */
  def newFailedAddress(cause: Throwable): Address =
    Address.Failed(cause)
}
