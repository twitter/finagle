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
  def lookup(path: Path): Activity[NameTree[Path]]

  /**
   * Compose this [[com.twitter.finagle.Namer Namer]] with `next`;
   * the returned [[com.twitter.finagle.Namer Namer]]. The result is
   * the [[com.twitter.finagle.NameTree Alt]]-tree of both lookups,
   * with this namer's result first.
   */
  def orElse(next: Namer): Namer = Namer.OrElse(self, next)

  /**
   * Bind and evaluate the tree. BindAndEval is lazy with respect to 
   * [[com.twitter.finagle.NameTree.Alt Alt]], whose branches are evaluated
   * only when needed.
   */
  def bindAndEval(tree: NameTree[Path]): Var[Addr] =
    Namer.bindAndEval(this, tree).run map {
      case Activity.Ok(Some(addrs)) => Addr.Bound(addrs.toSet)
      case Activity.Ok(None) => Addr.Neg
      case Activity.Pending => Addr.Pending
      case Activity.Failed(exc) => Addr.Failed(exc)
    }

  /**
   * Perform one step of a [[com.twitter.finagle.NameTree.bind bind]]. This 
   * binds until a [[com.twitter.finagle.Namer Namer]] lookup. 
   * [[com.twitter.finagle.NameTree.bind Bind]] is equivalent to calling 
   * [[com.twitter.finagle.NameTree.bind1 bind1]] successively until a
   * fixpoint is reached.
   */
  def bind1(tree: NameTree[Path]): Activity[NameTree[Path]]  = 
    Namer.bind1(this)(tree)
}

private case class FailingNamer(exc: Throwable) extends Namer {
  def lookup(path: Path): Activity[NameTree[Path]] = 
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

    def lookup(path: Path): Activity[NameTree[Path]] = path match {
      case Path.Utf8("$", "inet", _*) => Activity.value(Leaf(path))
      // Temporary hack until we make weights first-class in
      // NameTrees again.      
      case Path.Utf8("$", "inetw", _*) => Activity.value(Leaf(path))
      case Path.Utf8("$", "nil", _*) => Activity.value(Empty)
      case Path.Utf8("$", kind, rest@_*) =>
        namerOfKind(kind).lookup(Path.Utf8(rest:_*))
      case _ => Activity.value(Neg)
    }
  }

  private object IntegerString {
    def unapply(s: String): Option[Int] =
      Try(s.toInt).toOption
  }
  
  private object DoubleString {
    def unapply(s: String): Option[Double] =
      Try(s.toDouble).toOption
  }
  
  private type Res = Option[Seq[SocketAddress]]
  private val MaxDepth = 100

  /**
   * Bind and evaluate the tree. BindAndEval is lazy with respect to 
   * [[com.twitter.finagle.NameTree.Alt Alt]], whose branches are evaluated
   * only when needed.
   */
 private def bindAndEval(namer: Namer, tree: NameTree[Path]): Activity[Res] =
   bindAndEval(namer, 0)(tree)

  private def bindAndEval(namer: Namer, depth: Int)(tree: NameTree[Path]): Activity[Res] = 
    if (depth > MaxDepth)
      Activity.exception(new IllegalArgumentException("Max recursion level reached."))
    else tree match {
      // TODO: Residual paths. Also, this treatment of special
      // paths isn't very satisfying.
      case Leaf(Path.Utf8("$", "inet", "", IntegerString(port))) =>
        Activity.value(Some(Seq(new InetSocketAddress(port))))

      case Leaf(Path.Utf8("$", "inet", host, IntegerString(port))) =>
        Activity.value(Some(Seq(new InetSocketAddress(host, port))))

      // Temporary hack until we make weights first-class in
      // NameTrees again.
      case Leaf(Path.Utf8("$", "inetw", DoubleString(weight), host, IntegerString(port))) =>
        val ia = new InetSocketAddress(host, port)
        Activity.value(Some(Seq(WeightedSocketAddress(ia, weight))))

      case Leaf(p) => 
        namer.lookup(p) flatMap bindAndEval(namer, depth+1)

      case Neg => Activity.value(None)
      case Empty => Activity.value(Some(Seq.empty))

      case Union(trees@_*) => and(trees map bindAndEval(namer, depth+1))
  
      case Alt(fst, rest@_*) =>
        bindAndEval(namer, depth+1)(fst) flatMap {
          case None => bindAndEval(namer, depth+1)(Alt(rest:_*))
          case other => Activity.value(other)
        }
  
      case Alt() => Activity.value(None)
    }

  private def and(trees: Seq[Activity[Res]]): Activity[Res] = 
    Activity.collect(trees) map {
      case trees if trees exists (_.isDefined) =>
        Some(trees flatMap(_.getOrElse(Seq.empty)))
      case _ => None
    }

  /**
   * Perform one step of a [[com.twitter.finagle.NameTree.bind bind]]. This 
   * binds until a [[com.twitter.finagle.Namer Namer]] lookup. 
   * [[com.twitter.finagle.NameTree.bind Bind]] is equivalent to calling 
   * [[com.twitter.finagle.NameTree.bind1 bind1]] successively until a
   * fixpoint is reached.
   */
  private def bind1(namer: Namer)(tree: NameTree[Path]): Activity[NameTree[Path]] = 
    tree match {
      case Leaf(p) => namer.lookup(p)
      case Union(trees@_*) => Activity.collect(trees map bind1(namer)) map Union.fromSeq
      case Alt(trees@_*) => Activity.collect(trees map bind1(namer)) map Alt.fromSeq
      case Neg => Activity.value(Neg)
      case Empty => Activity.value(Empty)
    }

  private case class OrElse(fst: Namer, snd: Namer) extends Namer {
    def lookup(path: Path): Activity[NameTree[Path]] = 
      (fst.lookup(path) join snd.lookup(path)) map { 
        case (left, right) => NameTree.Alt(left, right) 
      }
  }
}

/**
 * A name which is to be interpreted with the given namer, which in
 * turn may be thread- or request-local.
 *
 * A naïve client may simply bind() the name prior to each request
 * dispatch; however this will quickly become expensive since clients
 * would need to be created for each request. Instead,
 * [[com.twitter.finagle.factory.InterpreterFactory
 * InterpreterFactory]] should be used: it is capable of
 * interepreting such names efficiently on a per-request basis.
 */
case class UninterpretedName(
    getNamer: () => Namer, 
    tree: NameTree[Path]) 
  extends Name {
  def bind(): Var[Addr] = getNamer().bindAndEval(tree)
  def reified: String = tree.show
}
