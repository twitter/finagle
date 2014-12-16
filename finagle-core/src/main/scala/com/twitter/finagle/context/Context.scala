package com.twitter.finagle.context

import com.twitter.util.Local

/**
 * A context contains a number of let-delimited bindings. Bindings
 * are indexed by type Key[A] in a typesafe manner. Later bindings
 * shadow earlier ones.
 *
 * Note that the implementation of context maintains all bindings 
 * in a linked list; context lookup requires a linear search.
 */
trait Context {
  type Key[A]
  
  trait Env {
    /** 
     * Retrieve the current definition of a key.
     *
     * @throws NoSuchElementException when the key is undefined
     * in this environment.
     */
    @throws[NoSuchElementException]("If the key does not exist")
    def apply[A](key: Key[A]): A

    /**
     * Retrieve the current definition of a key, but only 
     * if it is defined.
     */
    def get[A](key: Key[A]): Option[A]

    /**
     * Tells whether `key` is defined in this environment.
     */
    def contains[A](key: Key[A]): Boolean
  
    /**
     * Create a derived environment where `key` is bound to
     * `value`; previous bindings of `key` are shadowed.
     */
    final def bound[A](key: Key[A], value: A): Env = Bound(this, key, value)
    
    /**
     * Clear the binding for the given key. Lookups for `key` will be 
     * negative in the returned environment.
     */
    final def cleared(key: Key[_]): Env = Cleared(this, key)
  }

  /**
   * An empty environment. No keys are present.
   */
  object Empty extends Env {
    def apply[A](key: Key[A]) = throw new NoSuchElementException
    def get[A](key: Key[A]) = None
    def contains[A](key: Key[A]) = false
    override def toString = "<empty com.twitter.finagle.context.Env>"
  }
  
  /**
   * An environment with `key` bound to `value`; lookups for other keys
   * are forwarded to `next`.
   */
  case class Bound[A](next: Env, key: Key[A], value: A) extends Env {
    def apply[B](key: Key[B]): B =
      if (key == this.key) value.asInstanceOf[B]
      else next(key)

    def get[B](key: Key[B]): Option[B] =
      if (key == this.key) Some(value.asInstanceOf[B])
      else next.get(key)

    def contains[B](key: Key[B]): Boolean = 
      key == this.key || next.contains(key)

    override def toString = s"Bound($key, $value) :: $next"
  }
  
  /**
   * An environment without `key`. Lookups for other keys
   * are forwarded to `next.
   */
  case class Cleared(next: Env, key: Key[_]) extends Env {
    def apply[A](key: Key[A]) = 
      if (key == this.key) throw new NoSuchElementException
      else next(key)

    def get[A](key: Key[A])  =
      if (key == this.key) None
      else next.get(key)

    def contains[A](key: Key[A]) =
      key != this.key && next.contains(key)

    override def toString = s"Clear($key) :: $next"
  }

  /**
   * Concatenate two environments with left-hand side precedence.
   */
  case class OrElse(left: Env, right: Env) extends Env {
    def apply[A](key: Key[A]) =
      if (left.contains(key)) left.apply(key)
      else right.apply(key)

    def get[A](key: Key[A])  =
      if (left.contains(key)) left.get(key)
      else right.get(key)

    def contains[A](key: Key[A]) =
      left.contains(key) || right.contains(key)

    override def toString = s"OrElse($left, $right)"
  }

  private[this] val local = new Local[Env]
  
  private[finagle] def env: Env = local() match {
    case Some(env) => env
    case None => Empty
  }
  
  /**
   * Retrieve the current definition of a key.
   *
   * @throws NoSuchElementException when the key is undefined
   * in the current request-local context.
   */
  @throws[NoSuchElementException]("If the key does not exist")
  def apply[A](key: Key[A]): A = env(key)

  /**
   * Retrieve the current definition of a key, but only 
   * if it is defined in the current request-local context.
   */
  def get[A](key: Key[A]): Option[A] = env.get(key)

  /**
   * Tells whether `key` is defined in the current request-local
   * context.
   */
  def contains[A](key: Key[A]): Boolean = env.contains(key)

  /**
   * Bind `value` to `key` in the scope of `fn`.
   */
  def let[A, R](key: Key[A], value: A)(fn: => R): R =
    local.let(env.bound(key, value))(fn)

  /**
   * Bind two keys and values in the scope of `fn`.
   */
  def let[A, B, R](key1: Key[A], value1: A, key2: Key[B], value2: B)(fn: => R): R =
    local.let(env.bound(key1, value1).bound(key2, value2))(fn)

  /**
   * Bind the given environment.
   */
  private[finagle] def let[R](env1: Env)(fn: => R): R =
    local.let(OrElse(env1, env))(fn)

  /**
   * Unbind the passed-in keys, in the scope of `fn`.
   */
  def letClear[R](keys: Key[_]*)(fn: => R): R = {
    val newEnv = keys.foldLeft(env) {
      case (e, k) => e.cleared(k)
    }
    local.let(newEnv)(fn)
  }
}
