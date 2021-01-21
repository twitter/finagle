package com.twitter.finagle

import java.net.{InetAddress, InetSocketAddress}
import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import scala.util.control.NoStackTrace
import scala.util.hashing.MurmurHash3

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
   * An ordering of [[Address Addresses]] based on a deterministic hash of their IP and port.
   *
   * @note In order to keep this hash ordering deterministic, it's important that
   * we avoid hash collisions for non-equal inputs. We do this by using a murmur hash
   * under the hood which is known to not have collisions for 32-bit inputs. However,
   * if the underlying addresses were to be 128-bit (IPv6), we would lose that
   * property.
   */
  def hashOrdering(seed: Int): Ordering[Address] = new Ordering[Address] {
    private[this] def hash(data: Int): Int = {
      // We effectively unroll what MurmurHash3.bytesHash does for a single
      // iteration. That is, it packs groups of four bytes into an int and
      // mixes then finalizes.
      MurmurHash3.finalizeHash(
        hash = MurmurHash3.mixLast(seed, data),
        // we have four bytes in `data`
        length = 4
      )
    }
    def compare(a0: Address, a1: Address): Int = (a0, a1) match {
      case (Address.Inet(inet0, _), Address.Inet(inet1, _)) =>
        if (inet0.isUnresolved && inet1.isUnresolved) {
          inet0.toString.compareTo(inet1.toString)
        } else if (inet0.isUnresolved) {
          -1
        } else if (inet1.isUnresolved) {
          1
        } else {
          val ipHash0 = MurmurHash3.bytesHash(inet0.getAddress.getAddress, seed)
          val ipHash1 = MurmurHash3.bytesHash(inet1.getAddress.getAddress, seed)
          val ipCompare = Integer.compare(ipHash0, ipHash1)
          if (ipCompare != 0) ipCompare
          else {
            Integer.compare(hash(inet0.getPort), hash(inet1.getPort))
          }
        }
      case (_: Address.Inet, _) => -1
      case (_, _: Address.Inet) => 1
      case _ => 0
    }

    override def toString: String = s"HashOrdering($seed)"
  }

  /**
   * An address represented by an Internet socket address.
   */
  case class Inet(addr: InetSocketAddress, metadata: Addr.Metadata) extends Address

  /**
   * An address that fails with the given `cause`.
   */
  case class Failed(cause: Throwable) extends Address

  /**
   * An endpoint address represented by a [[com.twitter.finagle.ServiceFactory]]
   * that implements the endpoint.
   */
  case class ServiceFactory[Req, Rep](
    factory: com.twitter.finagle.ServiceFactory[Req, Rep],
    metadata: Addr.Metadata)
      extends Address

  object ServiceFactory {
    def apply[Req, Rep](
      factory: com.twitter.finagle.ServiceFactory[Req, Rep]
    ): ServiceFactory[Req, Rep] = ServiceFactory(factory, Addr.Metadata.empty)
  }

  /** Create a new [[Address]] with given [[java.net.InetSocketAddress]]. */
  def apply(addr: InetSocketAddress): Address =
    Address.Inet(addr, Addr.Metadata.empty)

  /** Create a new [[Address]] with given `host` and `port`. */
  def apply(host: String, port: Int): Address =
    Address(new InetSocketAddress(host, port))

  /** Create a new loopback [[Address]] with the given `port`. */
  def apply(port: Int): Address =
    Address(new InetSocketAddress(InetAddress.getLoopbackAddress, port))

  /** Create a new [[Address]] with the given [[com.twitter.finagle.ServiceFactory]]. */
  def apply[Req, Rep](factory: com.twitter.finagle.ServiceFactory[Req, Rep]): Address =
    Address.ServiceFactory(factory, Addr.Metadata.empty)

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
