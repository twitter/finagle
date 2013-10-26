package com.twitter.finagle

import collection.immutable
import java.net.SocketAddress
import com.twitter.util.Var
import com.twitter.finagle.util.Path

/**
 * A name identifies an object. Names may be resolved from strings
 * via [[com.twitter.finagle.Resolver Resolver]]s. Names are late
 * bound via `bind`.
 *
 * Names are typically used to identify service endpoints, and are
 * used in trait [[com.twitter.finagle.Client Client]] to name
 * destinations-- i.e. where client traffic terminates.
 */
trait Name {
  /**
   * Bind the name. The bound name is returned as a 
   * variable representation -- it is subject to change at
   * any time.
   */
  def bind(): Var[Addr]

  /**
   * Scope the name by the given path.
   */
  def enter(path: String): Name = 
    if (path.isEmpty) this else PartialName(this, path)

  /**
   * The reified version of the Name -- a resolvable string.
   */
  def reified: String
  
  override def toString = "Name("+reified+")"
}

object Name {
  /**
   * Create a pre-bound address.
   */
  def bound(addrs: SocketAddress*): Name = new Name {
    def bind() = Var.value(Addr.Bound(addrs:_*))
    val reified = "inet!"+(addrs mkString ",")
  }

  /**
   * Create a name from a group.
   *
   * @note Full Addr semantics cannot be recovered from Group. We
   * take a conservative approach here: we will only provide bound
   * addresses. Empty sets could indicate either pending or negative
   * resolutions.
   */
  def fromGroup(g: Group[SocketAddress]): Name = new Name {
    def bind() = g.set map { newSet => Addr.Bound(newSet) }
    val reified = "fail!"
  }

  /**
   * Create a pure path Name.
   */
  def apply(path: String): Name = new Name {
    def bind() = Var.value(Addr.Delegated(path))
    override def enter(suffix: String) =
      Name(Path.join(path, suffix))
    val reified = path
  }
}

/** 
 * Represents a socket address that is partially resolved. The
 * residual path is stored in `path`, and must be passed along with
 * the client request.
 */
private case class PartialSocketAddress(
  sa: SocketAddress, 
  path: String
) extends SocketAddress

/**
 * Represents a name that is partial: bound addresses are 
 * mapped to partial addresses storing the residual path.
 */
private case class PartialName(
  parent: Name, path: String
) extends Name {
  def bind(): Var[Addr] = parent.bind() map {
    case Addr.Bound(sockaddrs) =>
      val partial: Set[SocketAddress] = 
        sockaddrs map { sa => PartialSocketAddress(sa, path) }
      Addr.Bound(partial)
    case a => a
  }

  override def enter(path: String): Name = 
    PartialName(parent, Path.join(this.path, path))
  
  val reified = "fail!"
}
