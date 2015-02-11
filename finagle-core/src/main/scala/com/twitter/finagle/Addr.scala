package com.twitter.finagle

import com.twitter.util.Duration
import java.util.{List => JList, Collection => JCollection, Map => JMap}
import java.net.SocketAddress
import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.immutable

/**
 * An address identifies the location of an object--it is a bound
 * name. An object may be replicated, and thus bound to multiple
 * physical locations; it may be delegated to an unbound name.
 * (Similar to a symbolic link in Unix.)
 */
sealed trait Addr

/**
 * Note: There is a Java-friendly API for this object: [[com.twitter.finagle.Addrs]].
 */
object Addr {

  /** Address metadata */
  type Metadata = Map[String, Any]
  object Metadata {
    def apply(pairs: (String, Any)*): Metadata = Map(pairs:_*)
    val empty: Metadata = Map.empty
  }

  /**
   * A bound name. The object is replicated at each of the given
   * socket addresses.
   *
   * Bound addresses include an arbitrary Map of metadata that
   * Namers or Resolvers may set to provide additional configuration
   * (e.g. geographical information) to client stacks.
   *
   * Note: This currently protects the underlying addresses
   * from access since we want to add partially resolved addresses
   * in the future. At this point, the API will be fixed.
   */
  case class Bound(
    addrs: immutable.Set[SocketAddress],
    metadata: Metadata
  ) extends Addr

  /**
   * The address is failed: binding failed with
   * the given cause.
   */
  case class Failed(cause: Throwable) extends Addr

  /**
   * The binding action is still pending.
   */
  object Pending extends Addr {
    override def toString = "Pending"
  }

  /**
   * A negative address: the name could not be bound.
   */
  object Neg extends Addr {
    override def toString = "Neg"
  }

  object Bound {
    @varargs
    def apply(addrs: SocketAddress*): Addr = Bound(Set(addrs:_*), Metadata.empty)

    def apply(addrs: Set[SocketAddress]): Addr = Bound(addrs, Metadata.empty)
  }

  object Failed {
    def apply(why: String): Addr = Failed(new Exception(why))
  }
}

/**
 * A Java adaptation of the [[com.twitter.finagle.Addr]] companion object.
 */
object Addrs {

  /**
   * @see com.twitter.finagle.Addr.Bound
   */
  @varargs
  def newBoundAddr(addrs: SocketAddress*): Addr = Addr.Bound(addrs: _*)

  /**
   * @see com.twitter.finagle.Addr.Bound
   */
  def newBoundAddr(addrs: JCollection[SocketAddress]): Addr =
    Addr.Bound(addrs.asScala.toSet, Addr.Metadata.empty)

  /**
   * @see com.twitter.finagle.Addr.Bound
   */
  def newBoundAddr(addrs: JCollection[SocketAddress], metadata: JMap[String, Any]): Addr =
    Addr.Bound(addrs.asScala.toSet, metadata.asScala.toMap)

  /**
   * @see com.twitter.finagle.Addr.Failed
   */
  def newFailedAddr(cause: Throwable): Addr = Addr.Failed(cause)

  /**
   * @see com.twitter.finagle.Addr.Failed
   */
  def newFailedAddr(why: String): Addr = Addr.Failed(why)

  /**
   * @see com.twitter.finagle.Addr.Pending
   */
  val pendingAddr: Addr = Addr.Pending

  /**
   * @see com.twitter.finagle.Addr.Neg
   */
  val negAddr: Addr = Addr.Neg
}
