package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Closable, Future, Duration, Timer, Var}
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
 * '''Note:''' `T`s must be hashable, definining `hashCode` and
 * `equals` to ensure that maps have exactly-once semantics.
 *
 * '''Note:''' Groups are invariant because Scala's Sets are. In
 * the case of sets, this is an implementation artifact, and is
 * unfortunate, but it's better to keep things simpler and
 * consistent.
 */
@deprecated("Var[Addr], Name", "6.7.x")
trait Group[T] { outer =>
  // Group is needlessly complex due to it transitioning to
  // deprecation. In order to provide reasonable compatibility with
  // forthcoming structrures, we base the group implementation on Var
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
    set.observe { v => r.set(v) }
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
   * @return `this` mixed in with `NamedGroup`, named `n`
   */
  def named(n: String): Group[T] =
    new Group[T]
      with Proxy
      with NamedGroup
    {
      val self = outer
      val set = self.set
      val name = n
    }

  def +(other: Group[T]): Group[T] = new Group[T] {
    protected[finagle] val set = for { a <- outer.set; b <- other.set } yield a++b
  }

  override def toString = "Group(%s)".format(this() mkString ", ")
}

trait MutableGroup[T] extends Group[T] {
  def update(newMembers: Set[T])
}

/**
 * A mixin trait to assign a ``name`` to the group. This is used
 * to assign labels to groups that ascribe meaning to them.
 */
trait NamedGroup {
  def name: String
}

object NamedGroup {
  def unapply(g: Group[_]): Option[String] = g match {
    case n: NamedGroup => Some(n.name)
    case _ => None
  }
}

object Group {
  /**
   * Construct a `T`-typed static group from the given elements.
   *
   * @param staticMembers the members of the returned static group
   */
  def apply[T](staticMembers: T*): Group[T] = new Group[T] {
    protected[finagle] val set = Var(Set(staticMembers:_*))
  }
  
  def fromVarAddr(va: Var[Addr]): Group[SocketAddress] = new Group[SocketAddress] {
    protected[finagle] val set = va map {
      case Addr.Bound(sockaddrs) => sockaddrs
      case _ => Set[SocketAddress]()
    }
  }

  /**
   * The empty group of type `T`.
   */
  def empty[T]: Group[T] = Group()

  /**
   * Creates a mutable group of type `T`.
   *
   * @param initial the initial elements of the group
   */
  def mutable[T](initial: T*): MutableGroup[T] = new MutableGroup[T] {
    protected[finagle] val set = Var(Set(initial:_*))
    def update(newMembers: Set[T]) { set() = newMembers }
  }

  /**
   * Construct a (dynamic) `Group` from the given
   * [[com.twitter.finagle.builder.Cluster]]. Note that clusters
   * are deprecated, so this constructor acts as a temporary
   * bridge.
   */
  def fromCluster[T](underlying: Cluster[T]): Group[T] = {
    val (snap, edits) = underlying.snap
    new Group[T] {
      protected[finagle] val set = Var(snap.toSet)

      edits foreach { spool =>
        spool foreach {
          case Cluster.Add(t) => set() += t
          case Cluster.Rem(t) => set() -= t
        }
      }
    }
  }
}
