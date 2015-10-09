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
 *
 * In practice, clients use a [[com.twitter.finagle.Resolver]] to resolve a
 * destination name string into a `Name`. This is achieved by passing a
 * destination name into methods such as
 * [[com.twitter.finagle.builder.ClientBuilder ClientBuilder.dest]] or
 * the `newClient` method of the appropriate protocol object
 * (e.g. `Http.newClient(/s/org/servicename)`). These APIs use `Resolver` under
 * the hood to resolve the destination names into the `Name` representation
 * of the appropriate cluster.
 *
 * As names are bound, a [[com.twitter.finagle.Namer Namer]] may elect
 * to bind only a [[com.twitter.finagle.Name Name]] prefix, leaving an
 * unbound residual name to be processed by a downstream Namer.
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
   * are equal whenever their `id`s are. `id` identifies the `addr`
   * and not the `path`.  If the `id` is a [[com.twitter.finagle.Name.Path
   * Path]], it should only contain *bound*--not residual--path components.
   *
   * The `path` contains unbound residual path components that were not
   * processed during name resolution.
   */
  class Bound private(
    val addr: Var[Addr],
    val id: Any,
    val path: com.twitter.finagle.Path
  ) extends Name with Proxy {
    def self = id

    // Workaround for https://issues.scala-lang.org/browse/SI-4807
    def canEqual(that: Any) = true

    def idStr: String =
      id match {
        case path: com.twitter.finagle.Path => path.show
        case _ => id.toString
      }
  }

  object Bound {
    def apply(addr: Var[Addr], id: Any, path: com.twitter.finagle.Path): Name.Bound =
      new Bound(addr, id, path)

    def apply(addr: Var[Addr], id: Any): Name.Bound =
      apply(addr, id, com.twitter.finagle.Path.empty)

    def unapply(name: Name.Bound): Option[Var[Addr]] = Some(name.addr)

    /**
     * Create a singleton address, equal only to itself.
     */
    def singleton(addr: Var[Addr]): Name.Bound = Name.Bound(addr, new Object())
  }

  // So that we can print NameTree[Name]
  implicit val showable: Showable[Name] = new Showable[Name] {
    def show(name: Name) = name match {
      case Path(path) => path.show
      case bound@Bound(_) =>
        bound.id match {
          case id: com.twitter.finagle.Path => id.show
          case id => id.toString
        }
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

  // Create a name representing the union of the passed-in names.
  // Metadata is not preserved on bound addresses.
  private[finagle] def all(names: Set[Name.Bound]): Name.Bound =
    if (names.isEmpty) empty
    else if (names.size == 1) names.head
    else {
      val va = Var.collect(names map(_.addr)) map {
        case addrs if addrs.exists({case Addr.Bound(_, _) => true; case _ => false}) =>
          val sockaddrs = addrs.flatMap {
            case Addr.Bound(as, _) => as
            case _ => Set.empty: Set[SocketAddress]
          }.toSet
          Addr.Bound(sockaddrs, Addr.Metadata.empty)

        case addrs if addrs.forall(_ == Addr.Neg) => Addr.Neg
        case addrs if addrs.forall({case Addr.Failed(_) => true; case _ => false}) =>
          Addr.Failed(new Exception)

        case _ => Addr.Pending
      }

      val id = names map { case bound@Name.Bound(_) => bound.id }
      Name.Bound(va, id)
    }
}
