package com.twitter.finagle

import collection.immutable
import java.net.SocketAddress
import com.twitter.util.Var

/**
 * A name identifies an object. Names may be resolved from strings
 * via [[com.twitter.finagle.Resolver]]s. Names are late
 * bound via `bind`.
 *
 * Names are typically used to identify service endpoints, and are
 * used in trait [[com.twitter.finagle.Client]] to name
 * destinations-- i.e. where client traffic terminates.
 */
trait Name extends SocketAddress {
  /**
   * Bind the name. The bound name is returned as a
   * variable representation -- it is subject to change at
   * any time.
   */
  def bind(): Var[Addr]

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
  def fromGroup(g: Group[SocketAddress]): Name = g match {
    case NameGroup(n) => n
    case g =>
      new Name {
        def bind() = g.set map { newSet => Addr.Bound(newSet) }
        val reified = "fail!"
      }
  }

  /**
   * Create a path-based Name which is interpreted vis-à-vis
   * the current request-local delegation table.
   */
  def apply(path: Path): Name = {
    // TODO: avoid the extra allocation here 
    // (orElse construction).
    def getName() = Dtab() orElse Namer.global
    val tree = NameTree.Leaf(path)
    UninterpretedName(getName, tree)
  }

  /**
   * Create a path-based Name which is interpreted vis-à-vis
   * the current request-local delegation table.
   */
  def apply(path: String): Name =
    Name(Path.read(path))

  /**
   * Create a new name from the given `Var[Addr]`.
   */
  def apply(va: Var[Addr]): Name = 
    VAName(va)
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

  val reified = "fail!"
}

case class VAName(va: Var[Addr]) extends Name {
  def bind() = va
  val reified = "fail!"
}

