package com.twitter.finagle

import collection.immutable
import java.net.SocketAddress
import com.twitter.util.Var
import com.twitter.finagle.util.Showable

/**
 * A name represents a logical entity. Names are opaque and may be
 * bound to a (dynamic) set of socket addresses which terminate the
 * name.
 *
 * Names may be created from strings of the form
 *
 * {{{
 * resolver!arg
 * }}}
 *
 * which asks the resolver to interpret the given argument. For
 * example `inet!localhost:9090` is a name which binds to the socket
 * address `localhost:9090`.
 *
 * Names that begin with the character `/` are ''path'' names. These
 * are abstract names that require interpretation by a
 * [[com.twitter.finagle.Dtab Dtab]].
 */
trait Name extends SocketAddress {
  /**
   * Bind the name. The bound name is returned as a variable
   * representation -- it is subject to change at any time.
   */
  def bind(): Var[Addr]
  
  @deprecated("Use 'show' instead", "6.13.x")
  def reified = show

  def show: String

  override def toString = "Name("+show+")"

  // A temporary bridge API to wait for some other changes to land.
  // Do not use.
  def tempAPI_toGroup: Group[SocketAddress] = NameGroup(this)
}

object Name {

  // So that we can print NameTree[Name]
  implicit val showable: Showable[Name] = new Showable[Name] {
    def show(name: Name) = name.show
  }

  /**
   * Create a pre-bound address.
   */
  def bound(addrs: SocketAddress*): Name = BoundName(addrs.toSet)

  /** 
   * An always-empty name.
   */
  val empty: Name = bound()

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
        val show = "unknown"
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
    PathName(getName, tree)
  }

  /**
   * Create a path-based Name which is interpreted vis-à-vis
   * the current request-local delegation table.
   */
  def apply(path: String): Name = Resolver.eval(path)

  /**
   * Create a name from a variable address.
   */
  def apply(va: Var[Addr]): Name = new Name {
    def bind() = va
    val show = "unknown"
  }

  /**
   * Create a name representing the union of the passed-in
   * names.
   */
  def all(names: Set[Name]): Name = 
    if (names.isEmpty) empty
    else if (names.size == 1) names.head
    else AllName(names)
}

private case class AllName(names: Set[Name]) extends Name {
  assert(names.nonEmpty)

  def bind() = Var.collect(names map(_.bind())) map {
    case addrs if addrs.exists({case Addr.Bound(_) => true; case _ => false}) =>
      Addr.Bound((addrs flatMap {
        case Addr.Bound(as) => as
        case _ => Set.empty: Set[SocketAddress]
      }).toSet)

    case addrs if addrs.forall(_ == Addr.Neg) => Addr.Neg
    case addrs if addrs.forall({case Addr.Failed(_) => true; case _ => false}) =>
      Addr.Failed(new Exception)

    case _ => Addr.Pending
  }

  def show = "union"
}

private case class BoundName(addrs: Set[SocketAddress]) extends Name {
  private[this] val bound = Var.value(Addr.Bound(addrs))
  def bind() = bound
  val show = "bound"
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

  val show = "partial"
}
