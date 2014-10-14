package com.twitter.finagle

import scala.collection.mutable
import scala.collection.immutable

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
   * @note `head` does not give access to the value `T`, use `make` instead
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
      case Node(head, mk, _) => "Node(role = %s, description = %s)".format(head.role, head.description)
      case Leaf(head, t) => "Leaf(role = %s, description = %s)".format(head.role, head.description)
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
  case class Role(val name: String) {
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
     * The [[com.twitter.finagle.Stack.Role Role]] that the element can serve
     */
    val role: Stack.Role

    /**
     * The description of the functionality of the element
     */
    val description: String

    /**
     * The [[com.twitter.finagle.Stack.Param Params]] used to configure the element
     */
    def params: Map[String, String]
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
        val params = Map.empty[String, String]
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
  trait Params {
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
     * Produce a new parameter map, overriding any previous
     * `P`-typed value.
     */
    def +[P: Param](p: P): Params
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

      def +[P](p: P)(implicit param: Param[P]): Params =
        copy(map + (param -> p))
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
   * A convenient class to construct stackable modules. This variant
   * operates over stack values. Useful for building stack elements:
   *
   * {{{
   * def myNode = new Simple[Int=>Int]("myelem") {
   *   def make(params: Params, next: Int=>Int): (Int=>Int) = {
   *     val Multiplied(m) = params[Multiplier]
   *     i => next(i*m)
   *   }
   * }
   * }}}
   */
  abstract class Simple[T] extends Stackable[T] {
    type Params = Stack.Params
    def make(next: T)(implicit params: Params): T

    def toStack(next: Stack[T]) = {
      Node(this, (params, next) => Leaf(this, make(next.make(params))(params)), next)
    }
  }

  /**
   * A convenience class to construct stackable modules. This variant
   * operates over stacks. Useful for building stack elements:
   *
   * {{{
   * def myNode = new Module[Int=>Int]("myelem") {
   *   def make(params: Params, next: Stack[Int=>Int]): Stack[Int=>Int] = {
   *     val Multiplier(m) = params[Multiplier]
   *     if (m == 1) next // It's a no-op, skip it.
   *     else Stack.Leaf("multiply", i => next.make(params)(i)*m)
   *   }
   * }
   * }}}
   */
  abstract class Module[T] extends Stackable[T] {
    type Params = Stack.Params
    def make(next: Stack[T])(implicit params: Params): Stack[T]

    def toStack(next: Stack[T]) = {
      Node(this, (params, next) => make(next)(params), next)
    }
  }
}

/**
 * Produce a stack from a `T`-typed element.
 */
trait Stackable[T] extends Stack.Head {
  private val _params = mutable.Map.empty[String, String]

  def params: immutable.Map[String, String] = synchronized { _params.toMap }

  def toStack(next: Stack[T]): Stack[T]


  // Record the parameter names and values
  private def register(paramVal: Product): Unit = synchronized {
    // zip two lists, and pair any unmatched values with `padding`
    def zipWithPadding[T](l1: List[T], l2: List[T], padding: T): List[(T, T)] =
      (l1.length - l2.length) match {
        case 0 => l1 zip l2
        case x if x > 0 => l1 zip (l2 ++ List.fill(x)(padding))
        case x if x < 0 => (l1 ++ List.fill(-x)(padding)) zip l2
      }

    val paramValues = paramVal.productIterator.toList.map(_.toString)
    val paramNames = paramVal.getClass.getDeclaredFields().map(_.getName()).toList
    zipWithPadding(paramNames, paramValues, "<unknown>").map { case (k, v) => _params.put(k, v) }
  }

  // Using get[<param name>] when accessing parameters in Stackables causes
  // the parameter name and value to be recorded in the params map of the
  // Stackable.head. Per-module recorded parameters are shown by
  // [[com.twitter.server.TwitterServer TwitterServer]] at the admin endpoint
  // "/admin/clients/<client name>")
  // TODO: Replace signature with equivalent:
  //   def get[P <: Product : Stack.Param](implicit params: Stack.Params): P
  // Once upgraded to Scala 2.10 (2.9 does not support having both implicit
  // parameters and context bounds)
  protected def get[P <: Product](implicit param: Stack.Param[P], params: Stack.Params): P = {
    val paramVal = params[P]
    register(paramVal)
    paramVal
  }
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
      def toStackable(role: Stack.Role, fn: T => T): Stackable[T] = {
        val test = role
        new Stack.Simple[T] {
          val role = test
          val description = role.name
          def make(next: T)(implicit params: Stack.Params) = fn(next)
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

  override def toString = "Builder(%s)".format(stack)
}
