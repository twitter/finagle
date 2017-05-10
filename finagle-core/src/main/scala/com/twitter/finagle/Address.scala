package com.twitter.finagle

import java.net.{InetAddress, InetSocketAddress}
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
    Address.Failed(new IllegalArgumentException("failing") with NoStackTrace {
      override def toString: String = """IllegalArgumentException("failing")"""
    })

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
    Address(new InetSocketAddress(InetAddress.getLoopbackAddress, port))

  /**
   * An [[Ordering]] which orders [[Address Addresses]] based on their IP.
   *
   * @note this assumes that that the [[Address]] is resolved and returns a
   * non-deterministic ordering otherwise or may fail with certain sorting
   * implementations which require consistent results across comparisons.
   */
  val OctetOrdering: Ordering[Address] = new Ordering[Address] {
    private[this] def compareBytes(ba0: Array[Byte], ba1: Array[Byte]): Int = {
      // we move shorter IPs to the front (i.e. ipv4)
      if (ba0.length < ba1.length) return -1
      if (ba0.length > ba1.length) return 1

      var i = 0
      while (i < ba0.length) {
        val b0 = ba0(i).toInt & 0xFF
        val b1 = ba1(i).toInt & 0xFF
        val comp = Integer.compare(b0, b1)
        if (comp != 0) return comp
        i += 1
      }

      0
    }

    def compare(a0: Address, a1: Address): Int = (a0, a1) match {
      case (Inet(inet0, _), Inet(inet1, _)) =>
        if (inet0.isUnresolved || inet1.isUnresolved) 0 else {
          val ipCompare = compareBytes(
            inet0.getAddress.getAddress,
            inet1.getAddress.getAddress)
          if (ipCompare != 0) ipCompare else {
            Integer.compare(inet0.getPort, inet1.getPort)
          }
        }
      case (_: Inet, _) => -1
      case (_, _: Inet) => 1
      case _ => 0
    }
  }
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
