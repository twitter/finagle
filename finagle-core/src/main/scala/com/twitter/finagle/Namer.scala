package com.twitter.finagle

import com.twitter.util.{Var, Try, NonFatal, Memoize, Activity}
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
}

private case class FailingNamer(exc: Throwable) extends Namer {
  def lookup(path: Path): Activity[NameTree[Name]] =
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

    def lookup(path: Path): Activity[NameTree[Name]] = path match {
      case Path.Utf8("$", "inet", "", IntegerString(port)) =>
        Activity.value(Leaf(Name.bound(new InetSocketAddress(port))))

      case Path.Utf8("$", "inet", host, IntegerString(port)) =>
        Activity.value(Leaf(Name.bound(new InetSocketAddress(host, port))))

      case Path.Utf8("$", "nil", _*) => Activity.value(Empty)
      case Path.Utf8("$", kind, rest@_*) => namerOfKind(kind).lookup(Path.Utf8(rest:_*))
      case _ => Activity.value(Neg)
    }
    
    override def toString = "Namer.global"
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

  private def bind(namer: Namer, depth: Int)(tree: NameTree[Name])
  : Activity[NameTree[Name.Bound]] =
    if (depth > MaxDepth)
      Activity.exception(new IllegalArgumentException("Max recursion level reached."))
    else tree match {
      case Leaf(Name.Path(path)) => namer.lookup(path) flatMap bind(namer, depth+1)
      case Leaf(bound@Name.Bound(_)) => Activity.value(Leaf(bound))
      case Neg => Activity.value(Neg)
      case Empty => Activity.value(Empty)
      case Union() | Alt() => Activity.value(Neg)

      case Union(trees@_*) => 
        Activity.collect(trees map bind(namer, depth+1)) map Union.fromSeq

      case Alt(trees@_*) =>
        Activity.collect(trees map bind(namer, depth+1)) map Alt.fromSeq
    }

  private case class OrElse(fst: Namer, snd: Namer) extends Namer {
    def lookup(path: Path): Activity[NameTree[Name]] =
      (fst.lookup(path) join snd.lookup(path)) map {
        case (left, right) => NameTree.Alt(left, right)
      }
  }
}

