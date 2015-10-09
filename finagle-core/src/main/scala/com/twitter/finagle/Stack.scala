package com.twitter.finagle

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Stacks represent stackable elements of type T. It is assumed that
 * T-typed elements can be stacked in some meaningful way; examples
 * are functions (function composition) [[Filter Filters]] (chaining),
 * and [[ServiceFactory ServiceFactories]] (through
 * transformers). T-typed values are also meant to compose: the stack
 * itself materializes into a T-typed value.
 *
 * Stacks are persistent, allowing for nondestructive
 * transformations; they are designed to represent 'template' stacks
 * which can be configured in various ways before materializing the
 * stack itself.
 *
 * Note: Stacks are advanced and sometimes subtle. For expert use
 * only!
 */
sealed trait Stack[T] {
  import Stack._

  /**
   * The head field of the Stack composes all associated metadata
   * of the topmost element of the stack.
   *
   * @note `head` does not give access to the value `T`, use `make` instead.
   * @see [[Stack.Head]]
   */
  val head: Stack.Head

  /**
   * Materialize the current stack with the given parameters,
   * producing a `T`-typed value representing the current
   * configuration.
   */
  def make(params: Params): T

  /**
   * Transform one stack to another by applying `fn` on each element;
   * the map traverses on the element produced by `fn`, not the
   * original stack.
   */
  def transform(fn: Stack[T] => Stack[T]): Stack[T] =
    fn(this) match {
      case Node(head, mk, next) => Node(head, mk, next.transform(fn))
      case leaf@Leaf(_, _) => leaf
    }

  /**
    * Insert the given [[Stackable]] before the stack elements matching
    * the argument role. If no elements match the role, then an
    * unmodified stack is returned.
    */
  def insertBefore(target: Role, insertion: Stackable[T]): Stack[T] =
    this match {
      case Node(head, mk, next) if head.role == target =>
        insertion +: Node(head, mk, next.insertBefore(target, insertion))
      case Node(head, mk, next) =>
        Node(head, mk, next.insertBefore(target, insertion))
      case leaf@Leaf(_, _) => leaf
    }

  /**
    * Insert the given [[Stackable]] before the stack elements matching
    * the argument role. If no elements match the role, then an
    * unmodified stack is returned.  `insertion` must conform to
    * typeclass [[CanStackFrom]].
    */
  def insertBefore[U](target: Role, insertion: U)(implicit csf: CanStackFrom[U, T]): Stack[T] =
    insertBefore(target, csf.toStackable(target, insertion))

  /**
   * Insert the given [[Stackable]] after the stack elements matching
   * the argument role. If no elements match the role, then an
   * unmodified stack is returned.
   */
  def insertAfter(target: Role, insertion: Stackable[T]): Stack[T] = transform {
    case Node(head, mk, next) if head.role == target =>
      Node(head, mk, insertion +: next)
    case stk => stk
  }

  /**
   * Insert the given [[Stackable]] after the stack elements matching
   * the argument role. If no elements match the role, then an
   * unmodified stack is returned.  `insertion` must conform to
   * typeclass [[CanStackFrom]].
   */
  def insertAfter[U](target: Role, insertion: U)(implicit csf: CanStackFrom[U, T]): Stack[T] =
    insertAfter(target, csf.toStackable(target, insertion))

  /**
   * Remove all nodes in the stack that match the `target` role.
   * Leaf nodes are not removable.
   */
  def remove(target: Role): Stack[T] =
    this match {
      case Node(head, mk, next) =>
        if (head.role == target) next.remove(target)
        else Node(head, mk, next.remove(target))
      case leaf@Leaf(_, _) => leaf
    }

  /**
   * Replace any stack elements matching the argument role with a
   * given [[Stackable]]. If no elements match the role, then an
   * unmodified stack is returned.
   */
  def replace(target: Role, replacement: Stackable[T]): Stack[T] = transform {
    case n@Node(head, _, next) if head.role == target =>
      replacement +: next
    case stk => stk
  }

  /**
   * Replace any stack elements matching the argument role with a
   * given [[Stackable]]. If no elements match the role, then an
   * unmodified stack is returned. `replacement` must conform to
   * typeclass [[CanStackFrom]].
   */
  def replace[U](target: Role, replacement: U)(implicit csf: CanStackFrom[U, T]): Stack[T] =
    replace(target, csf.toStackable(target, replacement))

  /**
   * Traverse the stack, invoking `fn` on each element.
   */
  @tailrec
  final def foreach(fn: Stack[T] => Unit): Unit = {
    fn(this)
    this match {
      case Node(_, _, next) => next.foreach(fn)
      case Leaf(_, _) =>
    }
  }

  /**
   * Traverse the stack, until you find that pred has been evaluated to true.
   * If `pred` finds an element, return true, otherwise, false.
   */
  @tailrec
  final def exists(pred: Stack[T] => Boolean): Boolean = this match {
    case _ if pred(this) => true
    case Node(_, _, next) => next.exists(pred)
    case Leaf(_, _) => false
  }

  /**
   * Returns whether the stack contains a given role or not.
   */
  def contains(role: Stack.Role): Boolean = exists(_.head.role == role)

  /**
   * Enumerate each well-formed stack contained within this stack.
   */
  def tails: Iterator[Stack[T]] = {
    val buf = new mutable.ArrayBuffer[Stack[T]]
    foreach { buf += _ }
    buf.toIterator
  }

  /**
   * Produce a new stack representing the concatenation of `this`
   * with `right`. Note that this replaces the terminating element of
   * `this`.
   *
   * Alias for [[Stack.++]].
   */
  def concat(right: Stack[T]): Stack[T] =
    this ++ right

  /**
   * Produce a new stack representing the concatenation of `this`
   * with `right`. Note that this replaces the terminating element of
   * `this`.
   */
  def ++(right: Stack[T]): Stack[T] = this match {
    case Node(head, mk, left) => Node(head, mk, left++right)
    case Leaf(_, _) => right
  }

  /**
   * A copy of this Stack with `stk` prepended.
   *
   * An alias for [[Stack.+:]].
   */
  def prepend(stk: Stackable[T]): Stack[T] =
    stk +: this

  /**
   * A copy of this Stack with `stk` prepended.
   */
  def +:(stk: Stackable[T]): Stack[T] =
    stk.toStack(this)

  override def toString = {
    val elems = tails map {
      case Node(head, mk, _) => s"Node(role = ${head.role}, description = ${head.description})"
      case Leaf(head, t) => s"Leaf(role = ${head.role}, description = ${head.description})"
    }
    elems mkString "\n"
  }
}

/**
 * @see [[stack.nilStack]] for starting construction of an
 * empty stack for [[ServiceFactory]]s.
 */
object Stack {
  /**
   * Base trait for Stack roles. A stack's role is indicative of its
   * functionality. Roles provide a way to group similarly-purposed stacks and
   * slot stack elements into specific usages.
   *
   * TODO: CSL-869
   */
  case class Role(name: String) {
    // Override `toString` to return the flat, lowercase object name for use in stats.
    private[this] lazy val _toString = name.toLowerCase
    override def toString = _toString
  }

  /**
   * Trait encompassing all associated metadata of a stack element.
   * [[Stackable Stackables]] extend this trait.
   */
  trait Head {
    /**
     * The [[Stack.Role Role]] that the element can serve.
     */
    def role: Stack.Role

    /**
     * The description of the functionality of the element.
     */
    def description: String

    /**
     * The [[Stack.Param Params]] that the element
     * is interested in.
     */
    def parameters: Seq[Stack.Param[_]]
  }

  /**
   * Nodes materialize by transforming the underlying stack in
   * some way.
   */
  case class Node[T](head: Stack.Head, mk: (Params, Stack[T]) => Stack[T], next: Stack[T])
    extends Stack[T]
  {
    def make(params: Params) = mk(params, next).make(params)
  }

  object Node {
    /**
     * A constructor for a 'simple' Node.
     */
    def apply[T](head: Stack.Head, mk: T => T, next: Stack[T]): Node[T] =
      Node(head, (p, stk) => Leaf(head, mk(stk.make(p))), next)
  }

  /**
   * A static stack element; necessarily the last.
   */
  case class Leaf[T](head: Stack.Head, t: T) extends Stack[T] {
    def make(params: Params) = t
  }

  object Leaf {
    /**
     * If only a role is given when constructing a leaf, then the head
     * is created automatically
     */
    def apply[T](_role: Stack.Role, t: T): Leaf[T] = {
      val head = new Stack.Head {
        val role = _role
        val description = _role.toString
        val parameters = Nil
      }
      Leaf(head, t)
    }
  }

  /**
   * A typeclass representing P-typed elements, eligible as
   * parameters for stack configuration. Note that the typeclass
   * instance itself is used as the key in parameter maps; thus
   * typeclasses should be persistent:
   *
   * {{{
   * case class Multiplier(i: Int) {
   *   def mk(): (Multipler, Stack.Param[Multipler]) =
   *     (this, Multiplier.param)
   * }
   * object Multiplier {
   *   implicit val param = Stack.Param(Multiplier(123))
   * }
   * }}}
   *
   * The `mk()` function together with `Parameterized.configured`
   * provides a convenient Java interface.
   */
  trait Param[P] {
    def default: P
  }
  object Param {
    def apply[T](t: => T): Param[T] = new Param[T] {
      // Note, this is lazy to avoid potential failures during
      // static initialization.
      lazy val default = t
    }
  }

  /**
   * A parameter map.
   */
  trait Params extends Iterable[(Param[_], Any)] {
    /**
     * Get the current value of the P-typed parameter.
     */
    def apply[P: Param]: P

    /**
     * Returns true if there is a non-default value for
     * the P-typed parameter.
     */
    def contains[P: Param]: Boolean

    /**
     * Iterator of all `Param`s and their associated values.
     */
    def iterator: Iterator[(Param[_], Any)]

    /**
     * Produce a new parameter map, overriding any previous
     * `P`-typed value.
     */
    def +[P: Param](p: P): Params

    /**
     * Alias for [[addAll(Params)]].
     */
    def ++(ps: Params): Params =
      addAll(ps)

    /**
     * Produce a new parameter map, overriding any previously
     * mapped values.
     */
    def addAll(ps: Params): Params

  }

  object Params {
    private case class Prms(map: Map[Param[_], Any]) extends Params {
      def apply[P](implicit param: Param[P]): P =
        map.get(param) match {
          case Some(v) => v.asInstanceOf[P]
          case None => param.default
        }

      def contains[P](implicit param: Param[P]): Boolean =
        map.contains(param)

      def iterator: Iterator[(Param[_], Any)] =
        map.iterator

      def +[P](p: P)(implicit param: Param[P]): Params =
        copy(map + (param -> p))

      def addAll(ps: Params): Params =
        copy(map ++ ps.iterator)
    }

    /**
     * The empty parameter map.
     */
    val empty: Params = Prms(Map.empty)
  }

  /**
   * A mix-in for describing an object that is parameterized.
   */
  trait Parameterized[+T] {
    def params: Stack.Params

    def configured[P: Stack.Param](p: P): T =
      withParams(params+p)

    def configured[P](psp: (P, Stack.Param[P])): T = {
      val (p, sp) = psp
      configured(p)(sp)
    }

    def withParams(ps: Stack.Params): T
  }

  /**
   * Encodes transformations for stacks of
   * [[com.twitter.finagle.ServiceFactory ServiceFactories]] of
   * arbitrary `Req` and `Rep` types. Such transformations must be
   * indifferent to these types in order to typecheck.
   */
  trait Transformer {
    def apply[Req, Rep](stack: Stack[ServiceFactory[Req, Rep]]): Stack[ServiceFactory[Req, Rep]]
  }

  trait Transformable[+T] {
    /**
     * Transform the stack using the given `Transformer`.
     */
    def transformed(t: Transformer): T
  }

  /**
   * A convenience class to construct stackable modules. This variant
   * operates over stacks and the entire parameter map. The `ModuleN` variants
   * may be more convenient for most definitions as they operate over `T` types
   * and the parameter extraction is derived from type parameters.
   *
   * {{{
   * def myNode = new Module[Int=>Int]("myelem") {
   *   val role = "Multiplier"
   *   val description = "Multiplies values by a multiplier"
   *   val parameters = Seq(implicitly[Stack.Param[Multiplied]])
   *   def make(params: Params, next: Stack[Int=>Int]): Stack[Int=>Int] = {
   *     val Multiplier(m) = params[Multiplier]
   *     if (m == 1) next // It's a no-op, skip it.
   *     else Stack.Leaf("multiply", i => next.make(params)(i)*m)
   *   }
   * }
   * }}}
   */
  abstract class Module[T] extends Stackable[T] {
    def make(params: Params, next: Stack[T]): Stack[T]
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => make(prms, next), next)
  }

  /** A module of 0 parameters. */
  abstract class Module0[T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Nil
    def make(next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this, make(next.make(prms))), next)
  }

  /** A module of 1 parameter. */
  abstract class Module1[P1: Param, T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] =
      Seq(implicitly[Param[P1]])
    def make(p1: P1, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this, make(prms[P1], next.make(prms))), next)
  }

  /** A module of 2 parameters. */
  abstract class Module2[P1: Param, P2: Param, T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] =
      Seq(implicitly[Param[P1]], implicitly[Param[P2]])
    def make(p1: P1, p2: P2, next: T): T
    def toStack(next: Stack[T]) =
      Node(this, (prms, next) => Leaf(this,
        make(prms[P1], prms[P2], next.make(prms))), next)
  }

  /** A module of 3 parameters. */
  abstract class Module3[P1: Param, P2: Param, P3: Param, T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]])
    def make(p1: P1, p2: P2, p3: P3, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this,
        make(prms[P1], prms[P2], prms[P3], next.make(prms))), next)
  }

  /** A module of 4 parameters. */
  abstract class Module4[P1: Param, P2: Param, P3: Param, P4: Param, T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]])
    def make(p1: P1, p2: P2, p3: P3, p4: P4, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this,
        make(prms[P1], prms[P2], prms[P3], prms[P4], next.make(prms))), next)
  }

  /** A module of 5 parameters. */
  abstract class Module5[P1: Param, P2: Param, P3: Param, P4: Param, P5: Param, T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]],
      implicitly[Param[P5]])
    def make(p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this,
        make(prms[P1], prms[P2], prms[P3], prms[P4], prms[P5], next.make(prms))), next)
  }

  /** A module of 6 parameters. */
  abstract class Module6[P1: Param, P2: Param, P3: Param, P4: Param, P5: Param, P6: Param, T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]],
      implicitly[Param[P5]],
      implicitly[Param[P6]])
    def make(p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this,
        make(prms[P1], prms[P2], prms[P3], prms[P4], prms[P5], prms[P6], next.make(prms))), next)
  }

}

/**
 * Produce a stack from a `T`-typed element.
 */
trait Stackable[T] extends Stack.Head {
  def toStack(next: Stack[T]): Stack[T]
}

/**
 * A typeclass for "stackable" items. This is used by the
 * [[StackBuilder]] to provide a convenient interface for constructing
 * Stacks.
 */
@scala.annotation.implicitNotFound("${From} is not Stackable to ${To}")
trait CanStackFrom[-From, To] {
  def toStackable(role: Stack.Role, el: From): Stackable[To]
}

object CanStackFrom {
  implicit def fromFun[T]: CanStackFrom[T=>T, T] =
    new CanStackFrom[T=>T, T] {
      def toStackable(r: Stack.Role, fn: T => T): Stackable[T] = {
        new Stack.Module0[T] {
          val role = r
          val description = r.name
          def make(next: T) = fn(next)
        }
      }
    }
}

/**
 * StackBuilders are imperative-style builders for Stacks. It
 * maintains a stack onto which new elements can be pushed (defining
 * a new stack).
 *
 * @see [[stack.nilStack]] for starting construction of an
 * empty stack for [[ServiceFactory]]s.
 */
class StackBuilder[T](init: Stack[T]) {
  def this(role: Stack.Role, end: T) = this(Stack.Leaf(role, end))

  private[this] var stack = init

  /**
   * Push the stack element `el` onto the stack; el must conform to
   * typeclass [[CanStackFrom]].
   */
  def push[U](role: Stack.Role, el: U)(implicit csf: CanStackFrom[U, T]): this.type = {
    stack = csf.toStackable(role, el) +: stack
    this
  }

  /**
   * Push a [[Stackable]] module onto the stack.
   */
  def push(module: Stackable[T]): this.type = {
    stack = module +: stack
    this
  }

  /**
   * Get the current stack as defined by the builder.
   */
  def result: Stack[T] = stack

  /**
   * Materialize the current stack: equivalent to
   * `result.make()`.
   */
  def make(params: Stack.Params): T = result.make(params)

  override def toString = s"Builder($stack)"
}
