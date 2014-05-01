package com.twitter.finagle

import java.net.SocketAddress
import com.twitter.util.Var
import com.twitter.finagle.util.Showable

/**
 * Names identify network locations. They come in two varieties:
 *
 *  1. [[com.twitter.finagle.Name.Bound Bound]] names are concrete.
 *  They represent a changeable list of network endpoints
 *  (represented by [[com.twitter.finagle.Addr Addr]]s).
 *
 *  2. [[com.twitter.finagle.Name.Path Path]] names are unbound
 *  paths, representing an abstract location which must be resolved
 *  by some context, usually the [[com.twitter.finagle.Dtab Dtab]].
 */
sealed trait Name

object Name {
  /**
   * Path names comprise a [[com.twitter.finagle.Path Path]] denoting a
   * network location.
   */
  case class Path(path: com.twitter.finagle.Path) extends Name

  /**
   * Bound names comprise a changeable [[com.twitter.finagle.Addr
   * Addr]] which carries a host list of internet addresses.
   *
   * Equality of two Names is delegated to `id`. Two Bound instances
   * are equal whenever their `id`s are.
   */
  class Bound private(val addr: Var[Addr], val id: Any) extends Name with Proxy {
    def self = id

    // Workaround for https://issues.scala-lang.org/browse/SI-4807
    def canEqual(that: Any) = true
  }

  object Bound {
    def apply(addr: Var[Addr], id: Any): Name.Bound = new Bound(addr, id)
    def unapply(name: Name.Bound): Option[Var[Addr]] = Some(name.addr)

    /**
     * Create a singleton address, equal only to itself.
     */
    def singleton(addr: Var[Addr]): Name.Bound = Name.Bound(addr, new{})
  }

  // So that we can print NameTree[Name]
  implicit val showable: Showable[Name] = new Showable[Name] {
    def show(name: Name) = name match {
      case Path(path) => path.show
      case bound@Bound(_) => bound.id.toString
    }
  }

  /**
   * Create a pre-bound address.
   */
  def bound(addrs: SocketAddress*): Name.Bound = 
    Name.Bound(Var.value(Addr.Bound(addrs:_*)), addrs.toSet)

  /**
   * An always-empty name.
   */
  val empty: Name.Bound = bound()

  /**
   * Create a name from a group.
   *
   * @note Full Addr semantics cannot be recovered from Group. We
   * take a conservative approach here: we will only provide bound
   * addresses. Empty sets could indicate either pending or negative
   * resolutions.
   */
  def fromGroup(g: Group[SocketAddress]): Name.Bound = g match {
    case NameGroup(name) => name
    case group => Name.Bound({
       // Group doesn't support the abstraction of "not yet bound" so
       // this is a bit of a hack
       @volatile var first = true

       group.set map {
         case newSet if first && newSet.isEmpty => Addr.Pending
         case newSet =>
           first = false
           Addr.Bound(newSet)
       }
     }, group)
  }

  /**
   * Create a path-based Name which is interpreted vis-à-vis
   * the current request-local delegation table.
   */
  def apply(path: com.twitter.finagle.Path): Name = 
    Name.Path(path)

  /**
   * Create a path-based Name which is interpreted vis-à-vis
   * the current request-local delegation table.
   */
  def apply(path: String): Name =
    Name.Path(com.twitter.finagle.Path.read(path))

  /**
   * Create a name representing the union of the passed-in
   * names.
   */
  def all(names: Set[Name.Bound]): Name.Bound = 
    if (names.isEmpty) empty
    else if (names.size == 1) names.head
    else {
      val va = Var.collect(names map(_.addr)) map {
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
      
      val id = names map { case bound@Name.Bound(_) => bound.id }
      Name.Bound(va, id)
    }

  // A temporary bridge API to wait for some other changes to land.
  // Do not use.
  def DONOTUSE_nameToGroup(name: Name): Group[SocketAddress] = {
    val bound@Name.Bound(_) = name
    NameGroup(bound)
  }
}
