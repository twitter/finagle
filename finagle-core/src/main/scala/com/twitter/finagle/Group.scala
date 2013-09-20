package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Closable, Future, Duration, Timer}

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
trait Group[T] { outer =>
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
    @volatile var snap = Set[T]()
    @volatile var current = Set[U]()
    var mapped = Map[T, U]()

    def update() = synchronized {
      val old = snap
      snap = outer.members

      mapped ++= (snap &~ old) collect {
        case e if f.isDefinedAt(e) => e -> f(e)
      }
      mapped --= old &~ snap

      current = Set() ++ mapped.values
    }

    update()

    def members = {
      if (outer.members ne snap) synchronized {
        if (outer.members ne snap)
          update()
      }

      current
    }
  }

  /**
   * The current members of this group. If the group has not
   * changed, the same object (ie. object identity holds) is returned.
   */
  def members: Set[T]

  /** Synonymous to `members` */
  def apply(): Set[T] = members

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
      def members = self.members
      val name = n
    }

  def +(other: Group[T]): Group[T] = new Group[T] {
    @volatile private[this] var current = Set.empty[T]
    @volatile private[this] var outerMembers = Set.empty[T]
    @volatile private[this] var otherMembers = Set.empty[T]

    private[this] def update() = synchronized {
      outerMembers = outer.members
      otherMembers = other.members
      current = outerMembers ++ otherMembers
    }
    update()

    private[this] def hasChanged =
      ((outerMembers ne outer.members) || (otherMembers ne other.members))

    def members = {
      if (hasChanged) synchronized {
        if (hasChanged)
          update()
      }

      current
    }
  }

  override def toString = "Group(%s)".format(members mkString ", ")
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
  val timer = DefaultTimer.twitter
  val defaultBackoff = (Backoff.linear(100.milliseconds, 100.milliseconds) take 5) ++ Backoff.const(1.second)

  /**
   * Returns a Future that is satisfied when the
   * predicate is affirmed. The group is polled
   * with the given backoff strategy.
   */
  def pollUntilTrue[T](
    group: Group[T],
    p: Group[T] => Boolean,
    backoff: Stream[Duration] = defaultBackoff
  ): Future[Unit] = {
    def poll(ds: Stream[Duration]): Future[Unit] = {
      if (p(group)) Future.Done
      else timer.doLater(ds.head)(()) flatMap { _ => poll(ds.tail) }
    }
    poll(backoff)
  }

  /**
   * Construct a `T`-typed static group from the given elements.
   *
   * @param staticMembers the members of the returned static group
   */
  def apply[T](staticMembers: T*): Group[T] = new Group[T] {
    val members = Set(staticMembers:_*)
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
    @volatile private[this] var current: Set[T] = Set(initial:_*)
    def members = current
    def update(newMembers: Set[T]) { current = newMembers }
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
      @volatile var current: Set[T] = snap.toSet
      edits foreach { spool =>
        spool foreach {
          case Cluster.Add(t) => current += t
          case Cluster.Rem(t) => current -= t
        }
      }

      def members = current
    }
  }
}
