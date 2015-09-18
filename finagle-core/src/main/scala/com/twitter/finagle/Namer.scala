package com.twitter.finagle

import com.twitter.finagle.naming.NameInterpreter
import com.twitter.util._
import java.net.InetSocketAddress

/**
 * A namer is a context in which a [[com.twitter.finagle.NameTree
 * NameTree]] is bound. The context is provided by the
 * [[com.twitter.finagle.Namer.lookup lookup]] method, which
 * translates [[com.twitter.finagle.Path Paths]] into
 * [[com.twitter.finagle.NameTree NameTrees]]. Namers may represent
 * external processes, for example lookups through DNS or to ZooKeeper,
 * and thus lookup results are represented by a [[com.twitter.util.Activity Activity]].
 */
trait Namer { self =>
  import Namer._

  /**
   * Translate a [[com.twitter.finagle.Path Path]] into a
   * [[com.twitter.finagle.NameTree NameTree]].
   */
  def lookup(path: Path): Activity[NameTree[Name]]

  /**
   * Bind the given tree with this namer. Bind recursively follows
   * paths by looking them up in this namer. A recursion depth of up
   * to 100 is allowed.
   */
  def bind(tree: NameTree[Path]): Activity[NameTree[Name.Bound]] =
    Namer.bind(this.lookup, tree)
}

private case class FailingNamer(exc: Throwable) extends Namer {
  def lookup(path: Path): Activity[NameTree[Name]] =
    Activity.exception(exc)
}

object Namer  {
  import NameTree._

  private[finagle] val namerOfKind: (String => Namer) = Memoize { kind =>
    try Class.forName(kind).newInstance().asInstanceOf[Namer] catch {
      case NonFatal(exc) => FailingNamer(exc)
    }
  }

  /**
   * The global [[com.twitter.finagle.Namer Namer]]. It binds paths of the form
   *
   * {{{
   *   /$/classname/path...
   * }}}
   *
   * By reflecting in the Java class `classname` whose expected type is a
   * [[com.twitter.finagle.Namer Namer]] with a zero-arg constructor,
   * and passing the residual path to it. Lookups fail when `classname` does
   * not exist or cannot be constructed.
   *
   * The global namer also handles paths of the form
   *
   * {{{
   *   /$/nil/...
   * }}}
   *
   * to force empty resolutions.
   */
  val global: Namer = new Namer {

    private[this] object InetPath {
      def unapply(path: Path): Option[(InetSocketAddress, Path)] = path match {
        case Path.Utf8("$", "inet", host, IntegerString(port), residual@_*) =>
          Some((new InetSocketAddress(host, port), Path.Utf8(residual:_*)))
        case Path.Utf8("$", "inet", IntegerString(port), residual@_*) =>
          Some((new InetSocketAddress(port), Path.Utf8(residual:_*)))
        case _ => None
      }
    }

    private[this] object FailPath {
      val prefix = Path.Utf8("$", "fail")

      def unapply(path: Path): Boolean =
        path startsWith prefix
    }

    private[this] object NilPath {
      val prefix = Path.Utf8("$", "nil")

      def unapply(path: Path): Boolean =
        path startsWith prefix
    }

    private[this] object NamerPath {
      def unapply(path: Path): Option[(Namer, Path)] = path match {
        case Path.Utf8("$", kind, rest@_*) => Some((namerOfKind(kind), Path.Utf8(rest: _*)))
        case _ => None
      }
    }

    def lookup(path: Path): Activity[NameTree[Name]] = path match {
      // Clients may depend on Name.Bound ids being Paths which resolve
      // back to the same Name.Bound.
      case InetPath(addr, residual) =>
        val id = path.take(path.size - residual.size)
        Activity.value(Leaf(Name.Bound(Var.value(Addr.Bound(addr)), id, residual)))

      case FailPath() => Activity.value(Fail)
      case NilPath() => Activity.value(Empty)
      case NamerPath(namer, rest) => namer.lookup(rest)
      case _ => Activity.value(Neg)
    }

    override def toString = "Namer.global"
  }

  /**
   * Resolve a path to an address set (taking `dtab` into account).
   */
  def resolve(dtab: Dtab, path: Path): Var[Addr] =
    NameInterpreter.bind(dtab, path).map(_.eval).run.flatMap {
      case Activity.Ok(None) => Var.value(Addr.Neg)
      case Activity.Ok(Some(names)) => Name.all(names).addr
      case Activity.Pending => Var.value(Addr.Pending)
      case Activity.Failed(exc) => Var.value(Addr.Failed(exc))
    }

  /**
   * Resolve a path to an address set (taking [[Dtab.local]] into account).
   */
  def resolve(path: Path): Var[Addr] = resolve(Dtab.base ++ Dtab.local, path)

  /**
   * Resolve a path to an address set (taking [[Dtab.local]] into account).
   */
  def resolve(path: String): Var[Addr] =
    Try { Path.read(path) } match {
      case Return(path) => resolve(path)
      case Throw(e) => Var.value(Addr.Failed(e))
    }

  private object IntegerString {
    def unapply(s: String): Option[Int] =
      Try(s.toInt).toOption
  }

  private object DoubleString {
    def unapply(s: String): Option[Double] =
      Try(s.toDouble).toOption
  }

  private val MaxDepth = 100

  /**
   * Bind the given tree by recursively following paths and looking them
   * up with the provided `lookup` function. A recursion depth of up to
   * 100 is allowed.
   */
  def bind(
    lookup: Path => Activity[NameTree[Name]],
    tree: NameTree[Path]
  ): Activity[NameTree[Name.Bound]] =
    bind(lookup, 0)(tree map { path => Name.Path(path) })

  private[this] def bindUnion(
    lookup: Path => Activity[NameTree[Name]],
    depth: Int,
    trees: Seq[Weighted[Name]]
  ): Activity[NameTree[Name.Bound]] = {

    val weightedTreeVars: Seq[Var[Activity.State[NameTree.Weighted[Name.Bound]]]] = trees.map {
      case Weighted(w, t) =>
        val treesAct: Activity[NameTree[Name.Bound]] = bind(lookup, depth)(t)
        treesAct.map(Weighted(w, _)).run
    }

    val stateVar: Var[Activity.State[NameTree[Name.Bound]]] = Var.collect(weightedTreeVars).map {
      seq: Seq[Activity.State[NameTree.Weighted[Name.Bound]]] =>

      // - if there's at least one activity in Ok state, return the union of them
      // - if all activities are pending, the union is pending.
      // - if no subtree is Ok, and there are failures, retain the first failure.

      val oks = seq.collect {
        case Activity.Ok(t) => t
      }
      if (oks.isEmpty) {
        seq.collectFirst {
          case f@Activity.Failed(_) => f
        }.getOrElse(Activity.Pending)
      } else {
        Activity.Ok(Union.fromSeq(oks).simplified)
      }
    }
    new Activity(stateVar)
  }

  // values of the returned activity are simplified and contain no Alt nodes
  private def bind(lookup: Path => Activity[NameTree[Name]], depth: Int)(tree: NameTree[Name])
  : Activity[NameTree[Name.Bound]] =
    if (depth > MaxDepth)
      Activity.exception(new IllegalArgumentException("Max recursion level reached."))
    else tree match {
      case Leaf(Name.Path(path)) => lookup(path) flatMap bind(lookup, depth+1)
      case Leaf(bound@Name.Bound(_)) => Activity.value(Leaf(bound))

      case Fail => Activity.value(Fail)
      case Neg => Activity.value(Neg)
      case Empty => Activity.value(Empty)

      case Union() => Activity.value(Neg)
      case Union(Weighted(_, tree)) => bind(lookup, depth)(tree)
      case Union(trees@_*) => bindUnion(lookup, depth, trees)

      case Alt() => Activity.value(Neg)
      case Alt(tree) => bind(lookup, depth)(tree)
      case Alt(trees@_*) =>
        def loop(trees: Seq[NameTree[Name]]): Activity[NameTree[Name.Bound]] =
          trees match {
            case Nil => Activity.value(Neg)
            case Seq(head, tail@_*) =>
              bind(lookup, depth)(head) flatMap {
                case Fail => Activity.value(Fail)
                case Neg => loop(tail)
                case head => Activity.value(head)
              }
          }
        loop(trees)
    }
}

/**
 * Abstract [[Namer]] class for Java compatibility.
 */
abstract class AbstractNamer extends Namer

/**
 * Base-trait for Namers that bind to a local Service.
 *
 * Implementers with a 0-argument constructor may be named and
 * auto-loaded with `/$/pkg.cls` syntax.
 *
 * Note that this can't actually be accomplished in a type-safe manner
 * since the naming step obscures the service's type to observers.
 */
trait ServiceNamer[Req, Rep] extends Namer {

  protected def lookupService(path: Path): Option[Service[Req, Rep]]

  def lookup(path: Path): Activity[NameTree[Name]] = lookupService(path) match {
    case None =>
      Activity.value(NameTree.Neg)
    case Some(svc) =>
      val factory = ServiceFactory(() => Future.value(svc))
      val addr = Addr.Bound(ServiceFactorySocketAddress(factory))
      val name = Name.Bound(Var.value(addr), factory, path)
      Activity.value(NameTree.Leaf(name))
  }
}



package namer {
  final class global extends Namer {
    def lookup(path: Path) = Namer.global.lookup(path)
  }
}
