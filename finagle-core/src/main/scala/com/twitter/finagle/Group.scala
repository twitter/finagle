package com.twitter.finagle

import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.util.{InetSocketAddressUtil, LoadService}
import java.net.SocketAddress
import java.util.logging.Logger

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

  override def toString = "Group(%s)".format(members mkString ", ")
}

trait MutableGroup[T] extends Group[T] {
  def update(newMembers: Set[T])
}

trait GroupResolver extends (String => Group[SocketAddress]) {
  val scheme: String
}

class GroupResolverNotFoundException(scheme: String)
  extends Exception("Group resolver not found for scheme \"%s\"".format(scheme))

class GroupDestinationInvalid(dest: String)
  extends Exception("Group destination \"%s\" is not valid".format(dest))

object InetGroupResolver extends GroupResolver {
  val scheme = "inet"
  def apply(addr: String) = {
    val expanded = InetSocketAddressUtil.parseHosts(addr)
    Group[SocketAddress](expanded:_*)
  }
}

object Group {
  def apply[T](staticMembers: T*): Group[T] = new Group[T] {
    val members = Set(staticMembers:_*)
  }

  def empty[T]: Group[T] = Group()

  def mutable[T](initial: T*): MutableGroup[T] = new MutableGroup[T] {
    @volatile private[this] var current: Set[T] = Set(initial:_*)
    def members = current
    def update(newMembers: Set[T]) { current = newMembers }
  }

  def fromCluster[T](underlying: Cluster[T]): Group[T] = new Group[T] {
    val (snap, edits) = underlying.snap
    @volatile var current: Set[T] = snap.toSet
    edits foreach { spool =>
      spool foreach {
        case Cluster.Add(t) => current += t
        case Cluster.Rem(t) => current -= t
      }
    }

    def members = current
  }

  private lazy val resolvers = {
    val rs = LoadService[GroupResolver]()
    val log = Logger.getLogger(getClass.getName)
    val resolvers = Seq(InetGroupResolver) ++ rs
    for (r <- resolvers)
      log.info("GroupResolver[%s] = %s(%s)".format(r.scheme, r.getClass.getName, r))
    resolvers
  }

  def apply(dest: String): Group[SocketAddress] =
    dest.split("!", 2) match {
      case Array(scheme, addr) =>
        val r = resolvers.find(_.scheme == scheme) getOrElse {
          throw new GroupResolverNotFoundException(scheme)
        }

        r(addr)

      case Array(addr) =>
        InetGroupResolver(addr)

      case _ =>
        throw new GroupDestinationInvalid(dest)
  }
}
