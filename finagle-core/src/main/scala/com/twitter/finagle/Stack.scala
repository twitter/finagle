package com.twitter.finagle

import scala.collection.mutable

/**
 * Stacks represent stackable elements of type T. It is assumed that
 * T-typed elements can be stacked in some meaningful way; examples
 * are functions (function composition) [[com.twitter.finagle.Filter
 * Filters]] (chaining), and [[com.twitter.finagle.ServiceFactory
 * ServiceFactories]] (through transformers). T-typed values are also
 * meant to compose: the stack itself materializes into a T-typed
 * value.
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
   * @see [[com.twitter.finagle.Stack.Head]]
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
   * Replace any stack elements matching the argument role with a given
   * [[com.twitter.finagle.Stackable Stackable]]. If no elements match the
   * role, then an unmodified stack is returned.
   */
  def replace(target: Role, replacement: Stackable[T]): Stack[T] = transform {
    case n@Node(head, _, next) if head.role == target =>
      replacement +: next
    case stk => stk
  }

  /**
   * Replace any stack elements matching the argument role with a given
   * [[com.twitter.finagle.Stackable]]. If no elements match the
   * role, then an unmodified stack is returned. `replacement` must conform to
   * typeclass [[com.twitter.finagle.CanStackFrom]].
   */
  def replace[U](target: Role, replacement: U)(implicit csf: CanStackFrom[U, T]): Stack[T] =
    replace(target, csf.toStackable(target, replacement))

  /**
   * Traverse the stack, invoking `fn` on each element.
   */
  def foreach(fn: Stack[T] => Unit) {
    fn(this)
    this match {
      case Node(_, _, next) => next.foreach(fn)
      case Leaf(_, _) =>
    }
  }

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
   */
  def ++(right: Stack[T]): Stack[T] = this match {
    case Node(head, mk, left) => Node(head, mk, left++right)
    case Leaf(_, _) => right
  }

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
   * [[com.twitter.finagle.Stackable Stackables]] extend this trait.
   */
  trait Head {
    /**
     * The [[com.twitter.finagle.Stack.Role Role]] that the element can serve.
     */
    def role: Stack.Role

    /**
     * The description of the functionality of the element.
     */
    def description: String

    /**
     * The [[com.twitter.finagle.Stack.Param Params]] that the element
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
   * case class Multiplier(i: Int)
   * implicit object Multiplier extends Stack.Param[Multiplier] {
   *   val default = Multiplier(123)
   * }
   * }}}
   */
  trait Param[P] {
    def default: P
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

    def withParams(ps: Stack.Params): T
  }

  /**
   * A convenience class to construct stackable modules. This variant
   * operates over stacks and the entire parameter map. The `ModuleN` variants
   * may be more convenient for most definitions as they operate over `T` types
   * and the paramater extraction is derived from type parameters.
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
    def toStack(next: Stack[T]) =
      Node(this, (prms, next) => make(prms, next), next)
  }

  /** A module of 0 parameters. */
  abstract class Module0[T] extends Stackable[T] {
    final val parameters = Nil
    def make(next: T): T
    def toStack(next: Stack[T]) =
      Node(this, (prms, next) => Leaf(this, make(next.make(prms))), next)
  }

  /** A module of 1 parameter. */
  abstract class Module1[P1: Param, T] extends Stackable[T] {
    final val parameters = Seq(implicitly[Param[P1]])
    def make(p1: P1, next: T): T
    def toStack(next: Stack[T]) =
      Node(this, (prms, next) => Leaf(this, make(prms[P1], next.make(prms))), next)
  }

  /** A module of 2 parameters. */
  abstract class Module2[P1: Param, P2: Param, T] extends Stackable[T] {
    final val parameters = Seq(implicitly[Param[P1]], implicitly[Param[P2]])
    def make(p1: P1, p2: P2, next: T): T
    def toStack(next: Stack[T]) =
      Node(this, (prms, next) => Leaf(this,
        make(prms[P1], prms[P2], next.make(prms))), next)
  }

  /** A module of 3 parameters. */
  abstract class Module3[P1: Param, P2: Param, P3: Param, T] extends Stackable[T] {
    final val parameters = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]])
    def make(p1: P1, p2: P2, p3: P3, next: T): T
    def toStack(next: Stack[T]) =
      Node(this, (prms, next) => Leaf(this,
        make(prms[P1], prms[P2], prms[P3], next.make(prms))), next)
  }

  /** A module of 4 parameters. */
  abstract class Module4[P1: Param, P2: Param, P3: Param, P4: Param, T] extends Stackable[T] {
    final val parameters = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]])
    def make(p1: P1, p2: P2, p3: P3, p4: P4, next: T): T
    def toStack(next: Stack[T]) =
      Node(this, (prms, next) => Leaf(this,
        make(prms[P1], prms[P2], prms[P3], prms[P4], next.make(prms))), next)
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
 * [[com.twitter.finagle.StackBuilder StackBuilder]] to provide a
 * convenient interface for constructing Stacks.
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
 */
class StackBuilder[T](init: Stack[T]) {
  def this(role: Stack.Role, end: T) = this(Stack.Leaf(role, end))

  private[this] var stack = init

  /**
   * Push the stack element `el` onto the stack; el must conform to
   * typeclass [[com.twitter.finagle.CanStackFrom CanStackFrom]].
   */
  def push[U](role: Stack.Role, el: U)(implicit csf: CanStackFrom[U, T]): this.type = {
    stack = csf.toStackable(role, el) +: stack
    this
  }

  /**
   * Push a [[com.twitter.finagle.Stackable Stackable]] module onto
   * the stack.
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
