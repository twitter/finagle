package com.twitter.finagle

import scala.annotation.{implicitNotFound, tailrec}
import scala.collection.{immutable, mutable}

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
  def make(params: Params): T =
    this match {
      case Node(_, mk, next) => mk(params, next).make(params)
      case Leaf(_, t) => t
    }

  /**
   * Transform one stack to another by applying `fn` on each element;
   * the map traverses on the element produced by `fn`, not the
   * original stack. Prefer using [[map]] over [[transform]].
   */
  protected def transform(fn: Stack[T] => Stack[T]): Stack[T] =
    fn(this) match {
      case Node(hd, mk, next) => Node(hd, mk, next.transform(fn))
      case leaf @ Leaf(_, _) => leaf
    }

  /**
   * Transform one stack to another by applying `fn` on each element.
   */
  def map(fn: (Stack.Head, T) => T): Stack[T] =
    this match {
      case Node(hd, mk, next) =>
        def mk2(p: Params, stk: Stack[T]): Stack[T] =
          Leaf(stk.head, fn(hd, mk(p, stk).make(p)))
        Node(hd, mk2(_, _), next.map(fn))
      case Leaf(hd, t) =>
        Leaf(hd, fn(hd, t))
    }

  /**
   * Insert the given [[Stackable]] before the stack elements matching
   * the argument role. If no elements match the role, then an
   * unmodified stack is returned.
   */
  def insertBefore(target: Role, insertion: Stackable[T]): Stack[T] =
    this match {
      case Node(hd, mk, next) if hd.role == target =>
        insertion +: Node(hd, mk, next.insertBefore(target, insertion))
      case Node(hd, mk, next) =>
        Node(hd, mk, next.insertBefore(target, insertion))
      case leaf @ Leaf(_, _) => leaf
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
  def insertAfter(target: Role, insertion: Stackable[T]): Stack[T] =
    this match {
      case Node(hd, mk, next) if hd.role == target =>
        Node(hd, mk, insertion +: next.insertAfter(target, insertion))
      case Node(hd, mk, next) =>
        Node(hd, mk, next.insertAfter(target, insertion))
      case leaf @ Leaf(_, _) => leaf
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
      case Node(hd, mk, next) =>
        if (hd.role == target) next.remove(target)
        else Node(hd, mk, next.remove(target))
      case leaf @ Leaf(_, _) => leaf
    }

  /**
   * Replace any stack elements matching the argument role with a
   * given [[Stackable]]. If no elements match the role, then an
   * unmodified stack is returned.
   */
  def replace(target: Role, replacement: Stackable[T]): Stack[T] = transform {
    case Node(hd, _, next) if hd.role == target =>
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
    case Node(hd, mk, left) => Node(hd, mk, left ++ right)
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
   * A copy of this Stack with `element` prepended using the given [[Stack.Role]].
   */
  def prepend[U](role: Stack.Role, element: U)(implicit csf: CanStackFrom[U, T]): Stack[T] =
    prepend(csf.toStackable(role, element))

  /**
   * A copy of this Stack with `stk` prepended.
   */
  def +:(stk: Stackable[T]): Stack[T] =
    stk.toStack(this)

  /**
   * Drops the leading elements of the stack while the stack matches the
   * supplied predicate.
   *
   * If the entire stack matches the predicate, returns the leaf node.
   */
  @tailrec
  final def dropWhile(pred: Stack[T] => Boolean): Stack[T] =
    if (!pred(this)) {
      this
    } else {
      this match {
        case Node(_, _, next) => next.dropWhile(pred)
        case leaf @ Leaf(_, _) => leaf
      }
    }

  /**
   * Returns the next entry in the Stack, or [[None]] if it's a Leaf.
   */
  def tailOption: Option[Stack[T]] = this match {
    case Node(_, _, next) => Some(next)
    case Leaf(_, _) => None
  }

  override def toString: String = {
    val elems = tails map {
      case Node(hd, _, _) => s"Node(role = ${hd.role}, description = ${hd.description})"
      case Leaf(hd, _) => s"Leaf(role = ${hd.role}, description = ${hd.description})"
    }
    elems.mkString("\n")
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
   */
  case class Role(name: String) {
    // Override `toString` to return the flat, lowercase object name for use in stats.
    private[this] lazy val _toString = name.toLowerCase
    override def toString: String = _toString
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

  object Head {

    /**
     * Construct a basic `Head` instance from a `Role`
     */
    def apply(_role: Stack.Role): Stack.Head = new Head {
      def role: Role = _role
      def description: String = role.toString
      def parameters: Seq[Param[_]] = Nil
    }
  }

  private case class Leaf[T](head: Stack.Head, t: T) extends Stack[T]

  private case class Node[T](head: Stack.Head, mk: (Params, Stack[T]) => Stack[T], next: Stack[T])
      extends Stack[T]

  /**
   * Nodes materialize by transforming the underlying stack in
   * some way.
   */
  def node[T](head: Stack.Head, mk: (Params, Stack[T]) => Stack[T], next: Stack[T]): Stack[T] =
    Node(head, mk, next)

  /**
   * A constructor for a 'simple' Node.
   */
  def node[T](head: Stack.Head, mk: T => T, next: Stack[T]): Stack[T] =
    Node(head, (p, stk) => Leaf(head, mk(stk.make(p))), next)

  /**
   * A static stack element; necessarily the last.
   */
  def leaf[T](head: Stack.Head, t: T): Stack[T] = Leaf(head, t)

  /**
   * If only a role is given when constructing a leaf, then the head
   * is created automatically
   */
  def leaf[T](role: Stack.Role, t: T): Stack[T] = leaf(Head(role), t)

  /**
   * A typeclass representing P-typed elements, eligible as
   * parameters for stack configuration. Note that the typeclass
   * instance itself is used as the key in parameter maps; thus
   * typeclasses should be persistent:
   *
   * {{{
   * case class Multiplier(i: Int) {
   *   def mk(): (Multiplier, Stack.Param[Multiplier]) =
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

    /**
     * Compatibility method so the Param value is accessible from Java.
     */
    final def getDefault: P = default

    /**
     * Method invoked to generate a way to display a P-typed param, which takes the form
     * Seq[(key, () => value)], where `key` and `value` are the variable names and values for
     * public member variables in the class. The function `() => value` is invoked to display the
     * current value of a member variable.
     *
     * This should be overridden by param classes that do not implement [[scala.Product]]
     */
    def show(p: P): Seq[(String, () => String)] = Seq.empty
  }
  object Param {
    def apply[T](t: => T): Param[T] = new Param[T] {
      // Note, this is lazy to avoid potential failures during
      // static initialization.
      lazy val default: T = t
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
     *
     * Java users may find it easier to use `plus` below.
     */
    def +[P: Param](p: P): Params

    /**
     * Java-friendly API for `+`.
     *
     * The `Tuple2` can be created by calls to a `mk(): (P, Param[P])` method on parameters
     * (see [[com.twitter.finagle.service.TimeoutFilter.Param.mk()]] as an example).
     */
    def plus[P](typeAndParam: (P, Param[P])): Params =
      this.+(typeAndParam._1)(typeAndParam._2)

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
     *
     * Java users may find it easier to use `Stacks.EMPTY_PARAMS`.
     */
    val empty: Params = Prms(Map.empty)
  }

  /**
   * A mix-in for describing an object that is parameterized.
   */
  trait Parameterized[+T] {
    def params: Stack.Params

    /**
     * Add the parameter, `p`, to the current [[Params]].
     *
     * Java users may find it easier to use the `Tuple2` version below.
     */
    def configured[P](p: P)(implicit sp: Stack.Param[P]): T =
      withParams(params + p)

    /**
     * Java friendly API for `configured`.
     *
     * The `Tuple2` can often be created by calls to a `mk(): (P, Stack.Param[P])`
     * method on parameters (see
     * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory.Param.mk()]]
     * as an example).
     */
    def configured[P](psp: (P, Stack.Param[P])): T = {
      val (p, sp) = psp
      configured(p)(sp)
    }

    /**
     * Adds all parameters, `newParams`, to the current [[Params]].
     */
    def configuredParams(newParams: Stack.Params): T = {
      withParams(params ++ newParams)
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

  /**
   * Encodes parameter injection for [[Stack.Params]]
   */
  trait ParamsInjector {
    def apply(params: Stack.Params): Stack.Params
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
   * `ModuleParams` is similar, except it requires `parameters` to be declared.
   *
   * {{{
   * def myNode = new Module[Int=>Int]("myelem") {
   *   val role = "Multiplier"
   *   val description = "Multiplies values by a multiplier"
   *   val parameters = Seq(implicitly[Stack.Param[Multiplied]])
   *   def make(params: Params, next: Stack[Int=>Int]): Stack[Int=>Int] = {
   *     val Multiplier(m) = params[Multiplier]
   *     if (m == 1) next // It's a no-op, skip it.
   *     else Stack.leaf("multiply", i => next.make(params)(i)*m)
   *   }
   * }
   * }}}
   */
  abstract class Module[T] extends Stackable[T] {
    def make(params: Params, next: Stack[T]): Stack[T]
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => make(prms, next), next)
  }

  abstract class ModuleParams[T] extends Stackable[T] {
    def make(params: Params, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this, make(prms, next.make(prms))), next)
  }

  /** A module of 0 parameters. */
  abstract class Module0[T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Nil
    def make(next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this, make(next.make(prms))), next)
  }

  class NoOpModule[T](val role: Role, val description: String) extends Module0[T] {
    def make(next: T): T =
      next
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
    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Leaf(this, make(prms[P1], prms[P2], next.make(prms))), next)
  }

  /** A module of 3 parameters. */
  abstract class Module3[P1: Param, P2: Param, P3: Param, T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] =
      Seq(implicitly[Param[P1]], implicitly[Param[P2]], implicitly[Param[P3]])
    def make(p1: P1, p2: P2, p3: P3, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(
        this,
        (prms, next) => Leaf(this, make(prms[P1], prms[P2], prms[P3], next.make(prms))),
        next
      )
  }

  /** A module of 4 parameters. */
  abstract class Module4[P1: Param, P2: Param, P3: Param, P4: Param, T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]]
    )
    def make(p1: P1, p2: P2, p3: P3, p4: P4, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(
        this,
        (prms, next) => Leaf(this, make(prms[P1], prms[P2], prms[P3], prms[P4], next.make(prms))),
        next
      )
  }

  /** A module of 5 parameters. */
  abstract class Module5[P1: Param, P2: Param, P3: Param, P4: Param, P5: Param, T]
      extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]],
      implicitly[Param[P5]]
    )
    def make(p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(
        this,
        (prms, next) =>
          Leaf(this, make(prms[P1], prms[P2], prms[P3], prms[P4], prms[P5], next.make(prms))),
        next
      )
  }

  /** A module of 6 parameters. */
  abstract class Module6[P1: Param, P2: Param, P3: Param, P4: Param, P5: Param, P6: Param, T]
      extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]],
      implicitly[Param[P5]],
      implicitly[Param[P6]]
    )
    def make(p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(
        this,
        (prms, next) =>
          Leaf(
            this,
            make(prms[P1], prms[P2], prms[P3], prms[P4], prms[P5], prms[P6], next.make(prms))
          ),
        next
      )
  }

  /** A module of 7 parameters. */
  abstract class Module7[
    P1: Param,
    P2: Param,
    P3: Param,
    P4: Param,
    P5: Param,
    P6: Param,
    P7: Param,
    T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]],
      implicitly[Param[P5]],
      implicitly[Param[P6]],
      implicitly[Param[P7]]
    )
    def make(p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6, p7: P7, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(
        this,
        (prms, next) =>
          Leaf(
            this,
            make(
              prms[P1],
              prms[P2],
              prms[P3],
              prms[P4],
              prms[P5],
              prms[P6],
              prms[P7],
              next.make(prms))
          ),
        next
      )
  }

  /** A module of 8 parameters. */
  abstract class Module8[
    P1: Param,
    P2: Param,
    P3: Param,
    P4: Param,
    P5: Param,
    P6: Param,
    P7: Param,
    P8: Param,
    T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]],
      implicitly[Param[P5]],
      implicitly[Param[P6]],
      implicitly[Param[P7]],
      implicitly[Param[P8]]
    )
    def make(p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6, p7: P7, p8: P8, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(
        this,
        (prms, next) =>
          Leaf(
            this,
            make(
              prms[P1],
              prms[P2],
              prms[P3],
              prms[P4],
              prms[P5],
              prms[P6],
              prms[P7],
              prms[P8],
              next.make(prms))
          ),
        next
      )
  }

  /** A module of 9 parameters. */
  abstract class Module9[
    P1: Param,
    P2: Param,
    P3: Param,
    P4: Param,
    P5: Param,
    P6: Param,
    P7: Param,
    P8: Param,
    P9: Param,
    T] extends Stackable[T] {
    final val parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Param[P1]],
      implicitly[Param[P2]],
      implicitly[Param[P3]],
      implicitly[Param[P4]],
      implicitly[Param[P5]],
      implicitly[Param[P6]],
      implicitly[Param[P7]],
      implicitly[Param[P8]],
      implicitly[Param[P9]]
    )
    def make(p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6, p7: P7, p8: P8, p9: P9, next: T): T
    def toStack(next: Stack[T]): Stack[T] =
      Node(
        this,
        (prms, next) =>
          Leaf(
            this,
            make(
              prms[P1],
              prms[P2],
              prms[P3],
              prms[P4],
              prms[P5],
              prms[P6],
              prms[P7],
              prms[P8],
              prms[P9],
              next.make(prms))
          ),
        next
      )
  }

  /**
   * Add an element to the `Stack` that will transform the parameters at that
   * specific position.
   */
  trait TransformParams[T] extends Stackable[T] {

    /**
     * Transform the provided params for the remaining elements of the `Stack`
     * to consume.
     */
    def transform(params: Stack.Params): Stack.Params

    def toStack(next: Stack[T]): Stack[T] =
      Node(this, (prms, next) => Stack.leaf(role, next.make(transform(prms))), next)
  }
}

/**
 * StackTransformer is a standard mechanism for transforming the default
 * shape of the Stack. It is a [[Stack.Transformer]] with a name.
 * Registration and retrieval of transformers from global state is managed by
 * [[StackServer.DefaultTransformer]]. The transformers will run at
 * materialization time for Finagle servers, allowing users to mutate a Stack
 * in a consistent way.
 *
 * Warning: While it's possible to modify params with this API, it's strongly
 * discouraged. Modifying params via transformers creates subtle dependencies
 * between modules and makes it difficult to reason about the value of
 * params, as it may change depending on the module's placement in the stack.
 * Whenever possible, [[ClientParamsInjector]] should be used instead.
 */
abstract class StackTransformer extends Stack.Transformer {
  def name: String
  override def toString: String = s"StackTransformer(name=$name)"
}

/**
 * A [[TransformerCollection]] is a collection of transformers which are typically
 * used globally by a stack. For example, both StackClient and StackServer have
 * a global collection of [[StackTransformers]] which can be "injected" before
 * the stack is materialized for all clients/servers in a process.
 *
 * This is purely additive and there isn't a way to remove elements from this collection
 * to limit the type mutations allowed.
 */
abstract class StackTransformerCollection {
  @volatile private var underlying = immutable.Queue.empty[StackTransformer]

  def append(transformer: StackTransformer): Unit =
    synchronized { underlying = underlying :+ transformer }

  def transformers: Seq[StackTransformer] =
    underlying

  // Used for testing only! Allows us to clear any state that we set here
  // so we don't pollute other tests.
  private[finagle] def clear(): Unit = synchronized {
    underlying = immutable.Queue.empty[StackTransformer]
  }
}

/**
 * ClientsParamsInjector is the standard mechanism for injecting params into
 * the client.  It is a ``Stack.ParamsInjector`` with a name.  The injection
 * will run at materialization time for Finagle clients, so that the parameters
 * for a Stack will be injected in a consistent way.
 */
abstract class ClientParamsInjector extends Stack.ParamsInjector {
  def name: String
  override def toString: String = s"ClientParamsInjector(name=$name)"
}

/**
 * `Stack.Params` forwarder to provide a clean Java API.
 */
object StackParams {

  /**
   * Same as [[Stack.Params.empty]].
   */
  val empty: Stack.Params = Stack.Params.empty
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
@implicitNotFound("${From} is not Stackable to ${To}")
trait CanStackFrom[-From, To] {
  def toStackable(role: Stack.Role, el: From): Stackable[To]
}

object CanStackFrom {
  implicit def fromFun[T]: CanStackFrom[T => T, T] =
    new CanStackFrom[T => T, T] {
      def toStackable(r: Stack.Role, fn: T => T): Stackable[T] = {
        new Stack.Module0[T] {
          def role: Stack.Role = r
          def description: String = r.name
          def make(next: T): T = fn(next)
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
  def this(role: Stack.Role, end: T) = this(Stack.leaf(role, end))

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

  override def toString: String = s"Builder($stack)"
}
