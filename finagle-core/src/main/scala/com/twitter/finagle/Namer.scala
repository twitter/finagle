package com.twitter.finagle

import com.twitter.util._
import java.net.{InetSocketAddress, SocketAddress}

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
  import NameTree._

  /**
   * Translate a [[com.twitter.finagle.Path Path]] into a
   * [[com.twitter.finagle.NameTree NameTree]].
   */
  def lookup(path: Path): Activity[NameTree[Name]]

  /**
   * Enumerate entries in this namer by prefix.
   */
  def enum(prefix: Path): Activity[Dtab]

  /**
   * Compose this [[com.twitter.finagle.Namer Namer]] with `next`;
   * the returned [[com.twitter.finagle.Namer Namer]]. The result is
   * the [[com.twitter.finagle.NameTree Alt]]-tree of both lookups,
   * with this namer's result first.
   */
  def orElse(next: Namer): Namer = Namer.OrElse(self, next)

  /**
   * Bind the given tree with this namer. Bind recursively follows
   * paths by looking them up in this namer. A recursion depth of up
   * to 100 is allowed.
   */
  def bind(tree: NameTree[Path]): Activity[NameTree[Name.Bound]] =
    Namer.bind(this, tree)

  /**
   * Bind, then evaluate the given NameTree with this Namer. The result
   * is translated into a Var[Addr].
   */
  def bindAndEval(tree: NameTree[Path]): Var[Addr] =
    bind(tree).map(_.eval).run flatMap {
      case Activity.Ok(None) => Var.value(Addr.Neg)
      case Activity.Ok(Some(names)) => Name.all(names).addr
      case Activity.Pending => Var.value(Addr.Pending)
      case Activity.Failed(exc) => Var.value(Addr.Failed(exc))
    }

  /**
   * Expand a prefix of this Namer, producing a Dtab reflecting the
   * this prefixed namespace. The returned Dtab does not contain any
   * root entries (/=>...), and thus represents valid suffixes of the
   * given prefix.
   */
  final def expand(prefix: Path): Activity[Dtab] = Namer.expand(this, prefix)
}

private case class FailingNamer(exc: Throwable) extends Namer {
  def lookup(path: Path): Activity[NameTree[Name]] =
    Activity.exception(exc)

  def enum(prefix: Path): Activity[Dtab] =
    Activity.exception(exc)
}

object Namer  {
  import NameTree._
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
    val namerOfKind: (String => Namer) = Memoize {
      kind =>
        try Class.forName(kind).newInstance().asInstanceOf[Namer] catch {
          case NonFatal(exc) => FailingNamer(exc)
        }
    }

    private[this] object InetPath {
      def unapply(path: Path): Option[InetSocketAddress] = path match {
        case Path.Utf8("$", "inet", IntegerString(port)) =>
          Some(new InetSocketAddress(port))
        case Path.Utf8("$", "inet", host, IntegerString(port)) =>
          Some(new InetSocketAddress(host, port))
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
        case Path.Utf8(("$" | "#"), kind, rest@_*) => Some((namerOfKind(kind), Path.Utf8(rest: _*)))
        case _ => None
      }
    }

    def lookup(path: Path): Activity[NameTree[Name]] = path match {
      // Clients may depend on Name.Bound ids being Paths which resolve
      // back to the same Name.Bound.
      case InetPath(addr) => Activity.value(Leaf(Name.Bound(Var.value(Addr.Bound(addr)), path)))

      case FailPath() => Activity.value(Fail)
      case NilPath() => Activity.value(Empty)
      case NamerPath(namer, rest) => namer.lookup(rest)
      case _ => Activity.value(Neg)
    }

    def enum(prefix: Path) = prefix match {
      case FailPath() => Activity.value(Dtab.fail)
      case NilPath() => Activity.value(Dtab.empty)
      case NamerPath(namer, rest) => namer.enum(rest)
      case _ =>
        // Can't enumerate the 'inet' namespace
        Activity.exception(new UnsupportedOperationException)
    }

    override def toString = "Namer.global"
  }

  /**
   * Resolve a path to an address set (taking [[Dtab.local]] into account).
   */
  def resolve(path: Path): Var[Addr] = {
    val namer = (Dtab.base ++ Dtab.local) orElse global
    namer.bindAndEval(NameTree.Leaf(path))
  }

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

  private def bind(namer: Namer, tree: NameTree[Path]): Activity[NameTree[Name.Bound]] =
    bind(namer, 0)(tree map { path => Name.Path(path) })

  // values of the returned activity are simplified and contain no Alt nodes
  private def bind(namer: Namer, depth: Int)(tree: NameTree[Name])
  : Activity[NameTree[Name.Bound]] =
    if (depth > MaxDepth)
      Activity.exception(new IllegalArgumentException("Max recursion level reached."))
    else tree match {
      case Leaf(Name.Path(path)) => namer.lookup(path) flatMap bind(namer, depth+1)
      case Leaf(bound@Name.Bound(_)) => Activity.value(Leaf(bound))

      case Fail => Activity.value(Fail)
      case Neg => Activity.value(Neg)
      case Empty => Activity.value(Empty)

      case Union() => Activity.value(Neg)
      case Union(Weighted(_, tree)) => bind(namer, depth)(tree)
      case Union(trees@_*) =>
        Activity.collect(
          trees map { case Weighted(w, t) => bind(namer, depth)(t) map(Weighted(w, _)) }
        ) map { trees =>
          Union.fromSeq(trees).simplified
        }

      case Alt() => Activity.value(Neg)
      case Alt(tree) => bind(namer, depth)(tree)
      case Alt(trees@_*) =>
        def loop(trees: Seq[NameTree[Name]]): Activity[NameTree[Name.Bound]] =
          trees match {
            case Nil => Activity.value(Neg)
            case Seq(head, tail@_*) =>
              bind(namer, depth)(head) flatMap {
                case Fail => Activity.value(Fail)
                case Neg => loop(tail)
                case head => Activity.value(head)
              }
          }
        loop(trees)
    }

  private def expandTree(namer: Namer, depth: Int)(tree: NameTree[Path]): Activity[Dtab] = {
    if (depth > MaxDepth)
      return Activity.exception(new IllegalArgumentException("Max recursion level reached."))

    tree match {
      case NameTree.Union(trees@_*) =>
        if (trees.isEmpty) Activity.value(Dtab.empty) else {
          val expanded = Activity.collect(trees map {
            case Weighted(w, t) => expandTree(namer, depth+1)(t).map((w, _))
          })
          expanded map { dtabs => Dtab.union(dtabs:_*) }
        }
      case NameTree.Alt(trees@_*) =>
        if (trees.isEmpty) Activity.value(Dtab.empty) else {
          val expanded = Activity.collect(trees map expandTree(namer, depth+1))
          expanded map { dtabs => Dtab.alt(dtabs:_*) }
        }

      case NameTree.Leaf(path) => expandPath(namer, depth+1)(path)
      case NameTree.Fail => Activity.value(Dtab(Vector(Dentry(Path(), NameTree.Fail))))
      case NameTree.Neg => Activity.value(Dtab(Vector(Dentry(Path(), NameTree.Neg))))
      case NameTree.Empty => Activity.value(Dtab.empty)
    }
  }

  private def expandPath(namer: Namer, depth: Int)(prefix: Path): Activity[Dtab] = {
    if (depth > MaxDepth)
      return Activity.exception(new IllegalArgumentException("Max recursion level reached."))

    namer.enum(prefix) flatMap { dtab =>
      val dtabs = Activity.collect(dtab map {
        case Dentry(Path(), tree) => expandTree(namer, depth+1)(tree)
        case dentry => Activity.value(Vector(dentry))
      })

      dtabs map { dtabs => Dtab(dtabs.flatten) }
    }
  }

  private def expand(namer: Namer, prefix: Path): Activity[Dtab] =
    expandPath(namer, 0)(prefix).map(_.simplified)

  private case class OrElse(fst: Namer, snd: Namer) extends Namer {
    def lookup(path: Path): Activity[NameTree[Name]] =
      (fst.lookup(path) join snd.lookup(path)) map {
        case (left, right) => NameTree.Alt(left, right)
      }

    def enum(prefix: Path): Activity[Dtab] =
      (fst.enum(prefix) join snd.enum(prefix)) map {
        case (left, right) => left.alt(right)
      }
  }
}
