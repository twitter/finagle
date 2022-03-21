package com.twitter.finagle

import com.twitter.finagle.util.CachedHashCode
import com.twitter.finagle.util.Showable
import scala.annotation.tailrec

/**
 * Name trees represent a composite T-typed name whose interpretation
 * is subject to evaluation rules. Typically, a [[com.twitter.finagle.Namer Namer]]
 * is used to provide evaluation context for these trees.
 *
 *  - [[com.twitter.finagle.NameTree.Union]] nodes represent the union of several
 *  trees; a destination is reached by load-balancing over the sub-trees.
 *
 *  - [[com.twitter.finagle.NameTree.Alt Alt]] nodes represent a fail-over relationship
 *  between several trees; the first successful tree is picked as the destination. When
 *  the tree-list is empty, Alt-nodes evaluate to Empty.
 *
 *  - A [[com.twitter.finagle.NameTree.Leaf Leaf]] represents a T-typed leaf node;
 *
 *  - A [[com.twitter.finagle.NameTree.Neg Neg]] represents a negative location; no
 *  destination exists here.
 *
 *  - Finally, [[com.twitter.finagle.NameTree.Empty Empty]] trees represent an empty
 *  location: it exists but is uninhabited at this time.
 */
sealed trait NameTree[+T] {

  /**
   * Use `f` to map a T-typed NameTree to a U-typed one.
   */
  def map[U](f: T => U): NameTree[U] =
    NameTree.map(f)(this)

  /**
   * A parseable representation of the name tree; a
   * [[com.twitter.finagle.NameTree NameTree]] is recovered
   * from this string by
   * [[com.twitter.finagle.NameTree.read NameTree.read]].
   */
  def show(implicit showable: Showable[T]): String = NameTree.show(this)

  /**
   * A simplified version of this NameTree -- the returned
   * name tree is equivalent vis-à-vis evaluation. The returned
   * name also represents a fixpoint; in other words:
   *
   * {{{
   *   tree.simplified == tree.simplified.simplified
   * }}}
   */
  def simplified: NameTree[T] = NameTree.simplify(this)

  /**
   * Evaluate this NameTree with the default evaluation strategy. A
   * tree is evaluated recursively, Alt nodes are evaluated by
   * selecting its first nonnegative child.
   */
  def eval[U >: T]: Option[Set[U]] = NameTree.eval[U](this) match {
    case NameTree.Fail => None
    case NameTree.Neg => None
    case NameTree.Leaf(value) => Some(value)
    case _ => scala.sys.error("bug")
  }
}

/**
 * The NameTree object comprises
 * [[com.twitter.finagle.NameTree NameTree]] types as well
 * as binding and evaluation routines.
 */
object NameTree {

  /**
   * A [[com.twitter.finagle.NameTree NameTree]] representing
   * fallback; it is evaluated by picking the first nonnegative
   * (evaluated) subtree.
   */
  case class Alt[+T](trees: NameTree[T]*) extends NameTree[T] with CachedHashCode.ForCaseClass {
    override def toString: String = "Alt(%s)".format(trees mkString ",")
  }
  object Alt {
    private[finagle] def fromSeq[T](trees: Seq[NameTree[T]]): Alt[T] = Alt(trees: _*)
  }

  case class Weighted[+T](weight: Double, tree: NameTree[T]) extends CachedHashCode.ForCaseClass

  object Weighted {
    val defaultWeight = 1d
  }

  /**
   * A [[com.twitter.finagle.NameTree NameTree]] representing a
   * weighted union of trees. It is evaluated by returning the union
   * of its (recursively evaluated) children, leaving corresponding
   * weights unchanged. When all children are negative, the Union
   * itself evaluates negative.
   *
   * NameTree gives no semantics to weights (they are interpreted
   * higher in the stack) except to simplify away single-child Unions
   * regardless of weight.
   */
  case class Union[+T](trees: Weighted[T]*) extends NameTree[T] with CachedHashCode.ForCaseClass {
    override def toString: String = "Union(%s)".format(trees mkString ",")
  }
  object Union {
    private[finagle] def fromSeq[T](trees: Seq[Weighted[T]]): Union[T] = Union(trees: _*)
  }

  case class Leaf[+T](value: T) extends NameTree[T] with CachedHashCode.ForCaseClass

  /**
   * A failing [[com.twitter.finagle.NameTree NameTree]].
   */
  object Fail extends NameTree[Nothing] {
    override def toString: String = "Fail"
  }

  /**
   * A negative [[com.twitter.finagle.NameTree NameTree]].
   */
  object Neg extends NameTree[Nothing] {
    override def toString: String = "Neg"
  }

  /**
   * An empty [[com.twitter.finagle.NameTree NameTree]].
   */
  object Empty extends NameTree[Nothing] {
    override def toString: String = "Empty"
  }

  /**
   * Rewrite the paths in a tree for values defined by the given
   * partial function.
   */
  def map[T, U](f: T => U)(tree: NameTree[T]): NameTree[U] =
    tree match {
      case Union(trees @ _*) =>
        val trees1 = trees map { case Weighted(w, t) => Weighted(w, t.map(f)) }
        Union(trees1: _*)

      case Alt(trees @ _*) =>
        val trees1 = trees map map(f)
        Alt(trees1: _*)

      case Leaf(t) => Leaf(f(t))

      case Fail => Fail
      case Neg => Neg
      case Empty => Empty
    }

  /**
   * Simplify the given [[com.twitter.finagle.NameTree NameTree]],
   * yielding a new [[com.twitter.finagle.NameTree NameTree]] which
   * is evaluation-equivalent.
   */
  def simplify[T](tree: NameTree[T]): NameTree[T] = tree match {
    case Alt() => Neg
    case Alt(tree) => simplify(tree)
    case Alt(trees @ _*) =>
      @tailrec def loop(trees: Seq[NameTree[T]], accum: Seq[NameTree[T]]): Seq[NameTree[T]] =
        trees match {
          case Nil => accum
          case Seq(head, tail @ _*) =>
            simplify(head) match {
              case Fail => accum :+ Fail
              case Neg => loop(tail, accum)
              case head => loop(tail, accum :+ head)
            }
        }
      loop(trees, Nil) match {
        case Nil => Neg
        case Seq(head) => head
        case trees => Alt.fromSeq(trees)
      }

    case Union() => Neg
    case Union(Weighted(_, tree)) => simplify(tree)
    case Union(trees @ _*) =>
      @tailrec def loop(trees: Seq[Weighted[T]], accum: Seq[Weighted[T]]): Seq[Weighted[T]] =
        trees match {
          case Nil => accum
          case Seq(Weighted(w, tree), tail @ _*) =>
            simplify(tree) match {
              case Fail | Neg => loop(tail, accum)
              case tree => loop(tail, accum :+ Weighted(w, tree))
            }
        }
      loop(trees, Nil) match {
        case Nil => Neg
        case Seq(Weighted(_, tree)) => tree
        case trees => Union.fromSeq(trees)
      }

    case other => other
  }

  /**
   * A string parseable by [[com.twitter.finagle.NameTree.read NameTree.read]].
   */
  @tailrec
  def show[T: Showable](tree: NameTree[T]): String = tree match {
    case Union(Weighted(_, tree)) => show(tree)
    case Alt(tree) => show(tree)

    case Alt(trees @ _*) =>
      val trees1 = trees.map(show1(_))
      trees1 mkString " | "

    case _ => show1(tree)
  }

  @tailrec
  private def show1[T: Showable](tree: NameTree[T]): String = tree match {
    case Union(Weighted(_, tree)) => show1(tree)
    case Alt(tree) => show1(tree)

    case Union(trees @ _*) =>
      val trees1 = trees map {
        case Weighted(Weighted.defaultWeight, t) => showSimple(t)
        case Weighted(w, t) => f"${w}%.2f*${showSimple(t)}"
      }
      trees1 mkString " & "

    case Alt(_*) => showParens(tree)

    case _ => showSimple(tree)
  }

  @tailrec
  private def showSimple[T: Showable](tree: NameTree[T]): String = tree match {
    case Union(Weighted(_, tree)) => showSimple(tree)
    case Alt(tree) => showSimple(tree)

    case Union(_*) => showParens(tree)
    case Alt(_*) => showParens(tree)

    case Leaf(l) => Showable.show(l)

    case Fail => "!"
    case Neg => "~"
    case Empty => "$"
  }

  private def showParens[T: Showable](tree: NameTree[T]): String = s"(${show(tree)})"

  // return value is restricted to Fail | Neg | Leaf
  // NB: discards weights
  private def eval[T](tree: NameTree[T]): NameTree[Set[T]] = tree match {
    case Union() | Alt() => Neg
    case Alt(tree) => eval(tree)
    case Union(Weighted(_, tree)) => eval(tree)
    case Fail => Fail
    case Neg => Neg
    case Empty => Leaf(Set.empty)
    case Leaf(t) => Leaf(Set(t))

    case Union(trees @ _*) =>
      @tailrec def loop(trees: Seq[Weighted[T]], accum: Seq[Set[T]]): NameTree[Set[T]] =
        trees match {
          case Nil =>
            accum match {
              case Nil => Neg
              case _ => Leaf(accum.flatten.toSet)
            }
          case Seq(Weighted(_, head), tail @ _*) =>
            eval(head) match {
              case Fail | Neg => loop(tail, accum)
              case Leaf(value) => loop(tail, accum :+ value)
              case _ => scala.sys.error("bug")
            }
        }
      loop(trees, Nil)

    case Alt(trees @ _*) =>
      @tailrec def loop(trees: Seq[NameTree[T]]): NameTree[Set[T]] =
        trees match {
          case Nil => Neg
          case Seq(head, tail @ _*) =>
            eval(head) match {
              case Fail => Fail
              case Neg => loop(tail)
              case head @ Leaf(_) => head
              case _ => scala.sys.error("bug")
            }
        }
      loop(trees)
  }

  implicit def equiv[T]: Equiv[NameTree[T]] = new Equiv[NameTree[T]] {
    def equiv(t1: NameTree[T], t2: NameTree[T]): Boolean =
      simplify(t1) == simplify(t2)
  }

  /**
   * Parse a [[com.twitter.finagle.NameTree NameTree]] from a string
   * with concrete syntax
   *
   * {{{
   * tree       ::= name
   *                weight '*' tree
   *                tree '&' tree
   *                tree '|' tree
   *                '(' tree ')'
   *
   * name       ::= path | '!' | '~' | '$'
   *
   * weight     ::= [0-9]*\.?[0-9]+
   * }}}
   *
   * For example:
   *
   * {{{
   * /foo & /bar | /baz | $
   * }}}
   *
   * parses in to the [[com.twitter.finagle.NameTree NameTree]]
   *
   * {{{
   * Alt(Union(Leaf(Path(foo)),Leaf(Path(bar))),Leaf(Path(baz)),Empty)
   * }}}
   *
   * The production `path` is documented at [[com.twitter.finagle.Path.read Path.read]].
   *
   * @throws IllegalArgumentException when the string does not
   * represent a valid name tree.
   */
  def read(s: String): NameTree[Path] = NameTreeParsers.parseNameTree(s)
}
