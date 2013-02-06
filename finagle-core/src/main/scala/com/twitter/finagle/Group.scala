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

trait GroupResolver extends (String => Group[SocketAddress]) {
  val scheme: String
}

class GroupResolverNotFoundException(scheme: String)
  extends Exception("Group resolver not found for scheme \"%s\"".format(scheme))

class GroupTargetInvalid(target: String)
  extends Exception("Group target \"%s\" is not valid".format(target))

object InetGroupResolver extends GroupResolver {
  val scheme = "inet"
  def apply(addr: String) = {
    val expanded = InetSocketAddressUtil.parseHosts(addr)
    Group[SocketAddress](expanded:_*)
  }
}

object Group {
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

  private sealed trait Token
  private case class El(e: String) extends Token
  private object Eq extends Token
  private object Bang extends Token
  
  private def delex(ts: Seq[Token]) =
    ts map {
      case El(e) => e
      case Bang => "!"
      case Eq => "="
    } mkString ""

  private def lex(s: String) = {
    s.foldLeft(List[Token]()) {
      case (ts, '=') => Eq :: ts
      case (ts, '!') => Bang :: ts
      case (El(s) :: ts, c) => El(s+c) :: ts
      case (ts, c) => El(""+c) :: ts
    }
  }.reverse

  /**
   * Resolve a group from a target name, a string. Resolve uses
   * `GroupResolver`s to do this. These are loaded via the Java
   * [[http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html ServiceLoader]]
   * mechanism. The default resolver is "inet", resolving DNS
   * name/port pairs.
   *
   * Target names have a simple grammar: The name of the resolver
   * precedes the name of the address to be resolved, separated by
   * an exclamation mark ("bang"). For example: inet!twitter.com:80
   * resolves the name "twitter.com:80" using the "inet" resolver. If no
   * resolver name is present, the inet resolver is used.
   *
   * Names resolved by this mechanism are also a 
   * [[com.twitter.finagle.NamedGroup]]. By default, this name is 
   * simply the `target` string, but it can be overriden by prefixing
   * a name separated by an equals sign from the rest of the target.
   * For example, the target "www=inet!google.com:80" resolves
   * "google.com:80" with the inet resolver, but the returned group's
   * [[com.twitter.finagle.NamedGroup]] name is "www".
   */
  def resolve(target: String): Group[SocketAddress] = 
    new Group[SocketAddress] 
      with Proxy 
      with NamedGroup
    {
      val lexed = lex(target)

      val (name, stripped) = lexed match {
        case El(n) :: Eq :: rest => (n, rest)
        case Eq :: rest => ("", rest)
        case rest => (target, rest)
      }

      val self = stripped match {
        case (Eq :: _) | (Bang :: _) =>
          throw new GroupTargetInvalid(target)

        case El(scheme) :: Bang :: addr =>
          val r = resolvers.find(_.scheme == scheme) getOrElse {
            throw new GroupResolverNotFoundException(scheme)
          }
  
          r(delex(addr))

        case ts =>
          InetGroupResolver(delex(ts))
      }

      def members = self.members
  }
}
