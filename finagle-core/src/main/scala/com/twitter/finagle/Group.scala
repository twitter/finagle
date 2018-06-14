package com.twitter.finagle

import com.twitter.util._
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference

/**
 * A group is a dynamic set of `T`-typed values. It is used to
 * represent dynamic hosts and operations over such lists. Its
 * flexibility is derived from the ability to ''map'', creating
 * derived groups. The map operation ensures that each element is
 * mapped over exactly once, allowing side-effecting operations to
 * rely on this to implement safe semantics.
 *
 * '''Note:''' querying groups is nonblocking, which means that
 * derived groups are effectively eventually consistent.
 *
 * '''Note:''' `T`s must be hashable, defining `hashCode` and
 * `equals` to ensure that maps have exactly-once semantics.
 *
 * '''Note:''' Groups are invariant because Scala's Sets are. In
 * the case of sets, this is an implementation artifact, and is
 * unfortunate, but it's better to keep things simpler and
 * consistent.
 */
@deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "6.7.x")
trait Group[T] { outer =>
  // Group is needlessly complex due to it transitioning to
  // deprecation. In order to provide reasonable compatibility with
  // forthcoming structures, we base the group implementation on Var
  // while retaining its two key semantics:
  //
  //   (1) unchanged objects retain identity;
  //   (2) collect & map are idempotent.
  //
  // The following are semi-internal, to be accessed only by Finagle
  // itself.

  protected[finagle] val set: Var[Set[T]]

  // We use the ref here to preserve group semantics.  IE: retain object
  // identity to repeated calls to Group.members
  final protected[finagle] lazy val ref = {
    val r = new AtomicReference[Set[T]]()
    set.changes.register(Witness(r))
    r
  }

  /**
   * Create a new group by mapping each element of this group
   * with `f`. `f` is guaranteed to be invoked exactly once for each
   * element of the groups, even for dynamic groups.
   */
  def map[U](f: T => U): Group[U] = collect { case e => f(e) }

  /**
   * Create a new group by collecting each element of this group
   * with `f`. `f` is guaranteed to be invoked exactly once for each
   * element of the group, even for dynamic groups.
   */
  def collect[U](f: PartialFunction[T, U]): Group[U] = new Group[U] {
    var mapped = Map[T, U]()
    var last = Set[T]()
    protected[finagle] val set = outer.set map { set =>
      synchronized {
        mapped ++= (set &~ last) collect {
          case el if f.isDefinedAt(el) => el -> f(el)
        }
        mapped --= last &~ set
        last = set
      }

      mapped.values.toSet
    }
  }

  /**
   * The current members of this group. If the group has not
   * changed, the same object is returned. This allows a simple
   * object identity check to be performed to see if the Group has
   * been updated.
   */
  final def members: Set[T] = ref.get
  final def apply(): Set[T] = members

  /**
   * Name the group `n`.
   *
   * @return `this` mixed in with `LabelledGroup`, named `n`
   */
  def named(n: String): Group[T] = LabelledGroup(this, n)

  def +(other: Group[T]): Group[T] = new Group[T] {
    protected[finagle] val set = for { a <- outer.set; b <- other.set } yield a ++ b
  }

  override def toString = "Group(%s)".format(this() mkString ", ")
}

/**
 * A group that simply contains a name. Getting at the set binds the
 * name, but mostly this is to ship names under the cover of old
 * APIs. (And hopefully will be deprecated soon enough.)
 */
@deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "6.7.x")
private[finagle] case class NameGroup(name: Name.Bound) extends Group[SocketAddress] {
  protected[finagle] lazy val set: Var[Set[SocketAddress]] = name.addr map {
    case Addr.Bound(set, _) => set.collect { case Address.Inet(ia, _) => ia }
    case _ => Set()
  }
}

@deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "6.7.x")
trait MutableGroup[T] extends Group[T] {
  def update(newMembers: Set[T]): Unit
}

/**
 * A mixin trait to assign a ``name`` to the group. This is used
 * to assign labels to groups that ascribe meaning to them.
 */
@deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "6.7.x")
case class LabelledGroup[T](underlying: Group[T], name: String) extends Group[T] {
  protected[finagle] lazy val set: Var[Set[T]] = underlying.set
}

object Group {

  /**
   * Construct a `T`-typed static group from the given elements.
   *
   * @param staticMembers the members of the returned static group
   */
  @deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "2014-11-21")
  def apply[T](staticMembers: T*): Group[T] = new Group[T] {
    protected[finagle] val set = Var(Set(staticMembers: _*))
  }

  def fromVarAddr(va: Var[Addr]): Group[SocketAddress] = new Group[SocketAddress] {
    protected[finagle] val set: Var[Set[SocketAddress]] = va map {
      case Addr.Bound(addrs, _) => addrs.collect { case Address.Inet(ia, _) => ia }
      case _ => Set[SocketAddress]()
    }
  }

  def fromVar[T](v: Var[Set[T]]): Group[T] = new Group[T] {
    protected[finagle] val set = v
  }

  /**
   * The empty group of type `T`.
   */
  @deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "2014-11-21")
  def empty[T]: Group[T] = Group()

  /**
   * Creates a mutable group of type `T`.
   *
   * @param initial the initial elements of the group
   */
  @deprecated("Use `com.twitter.finagle.Name` to represent clusters instead", "2014-11-21")
  def mutable[T](initial: T*): MutableGroup[T] = new MutableGroup[T] {
    protected[finagle] val set = Var(Set(initial: _*))
    def update(newMembers: Set[T]): Unit = { set() = newMembers }
  }
}
