package com.twitter.finagle.context

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

  /**
   * Represents an opaque type that consists of a key-value pair
   * which can be used to set multiple keys in the context.
   */
  case class KeyValuePair[T](key: Key[T], value: T)

  /**
   * Retrieve the current definition of a key.
   *
   * @throws NoSuchElementException when the key is undefined
   * in the current request-local context.
   */
  @throws[NoSuchElementException]("If the key does not exist")
  def apply[A](key: Key[A]): A = get(key) match {
    case Some(v) => v
    case None => throw new NoSuchElementException(s"Key not found: ${key.toString}")
  }

  /**
   * Retrieve the current definition of a key, but only
   * if it is defined in the current request-local context.
   */
  def get[A](key: Key[A]): Option[A]

  /**
   * Retrieve the current definition of a key if it is defined.
   * If it is not defined, `orElse` is evaluated and returned.
   */
  def getOrElse[A](key: Key[A], orElse: () => A): A = get(key) match {
    case Some(a) => a
    case None => orElse()
  }

  /**
   * Tells whether `key` is defined in the current request-local
   * context.
   */
  def contains[A](key: Key[A]): Boolean = get(key).isDefined

  /**
   * Bind `value` to `key` in the scope of `fn`.
   */
  def let[A, R](key: Key[A], value: A)(fn: => R): R

  /**
   * Bind two keys and values in the scope of `fn`.
   */
  def let[A, B, R](key1: Key[A], value1: A, key2: Key[B], value2: B)(fn: => R): R

  /**
   * Bind multiple key-value pairs. Keys later in the collection take
   * precedent over keys earlier in the collection.
   */
  def let[R](keys: Iterable[KeyValuePair[_]])(fn: => R): R

  /**
   * Unbind the passed-in key, in the scope of `fn`.
   */
  def letClear[R](key: Key[_])(fn: => R): R

  /**
   * Unbind the passed-in keys, in the scope of `fn`.
   */
  def letClear[R](keys: Iterable[Key[_]])(fn: => R): R

  /**
   * Clears all bindings in the scope of `fn`.
   *
   * For example:
   * {{{
   *   context.let(Key1, "value1") {
   *     context.let(Key2, "something else") {
   *       context.letClearAll {
   *         // context.contains(Key1) == false
   *         // context.contains(Key2) == false
   *       }
   *       // context(Key1) == "value1"
   *       // context(Key2) == "something else"
   *     }
   *   }
   * }}}
   */
  def letClearAll[R](fn: => R): R
}
