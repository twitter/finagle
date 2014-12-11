package com.twitter.finagle

import java.util.{List => JList, Collection => JCollection}
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
  /**
   * A bound name. The object is replicated
   * at each of the given socket addresses.
   *
   * Note: This currently protects the underlying addresses
   * from access since we want to add partially resolved addresses
   * in the future. At this point, the API will be fixed.
   */
  case class Bound(addrs: immutable.Set[SocketAddress])
    extends Addr

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
    def apply(addrs: SocketAddress*): Addr =
      Bound(Set(addrs:_*))

    /**
     * Provided for Java compatibility.
     */
    def apply(addrs: JList[SocketAddress]): Addr = Addrs.newBoundAddr(addrs)
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
    Addr.Bound(addrs.asScala.toSet)

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
