package com.twitter.finagle.context

import com.twitter.util.Local

/**
 * A type of context that is local to the process. The type of Key is
 * also unique (generative) to each instance of this context, so that keys
 * cannot be used across different instances of this context type.
 */
final class LocalContext private[context] extends Context {

  private[this] val local = new Local[Map[Key[_], Any]]

  class Key[A]

  /**
   * A java-friendly key constructor.
   */
  def newKey[A]() = new Key[A]

  def get[A](key: Key[A]): Option[A] = env.get(key).asInstanceOf[Option[A]]

  def let[A, R](key: Key[A], value: A)(fn: => R): R =
    letLocal(env.updated(key, value))(fn)

  def let[A, B, R](key1: Key[A], value1: A, key2: Key[B], value2: B)(fn: => R): R = {
    val next = env.updated(key1, value1).updated(key2, value2)
    letLocal(next)(fn)
  }

  def let[R](pairs: Iterable[KeyValuePair[_]])(fn: => R): R = {
    val next = pairs.foldLeft(env) { case (env, KeyValuePair(k, v)) => env.updated(k, v) }
    letLocal(next)(fn)
  }

  def letClear[R](key: Key[_])(fn: => R): R = letLocal(env - key)(fn)

  def letClear[R](keys: Iterable[Key[_]])(fn: => R): R = {
    val next = keys.foldLeft(env) { (e, k) => e - k }
    letLocal(next)(fn)
  }

  def letClearAll[R](fn: => R): R = local.letClear(fn)

  // Exposed for testing
  private[context] def env: Map[Key[_], Any] = local() match {
    case Some(env) => env
    case None => Map.empty
  }

  // Exposed for testing
  private[context] def letLocal[T](env: Map[Key[_], Any])(fn: => T): T =
    local.let(env)(fn)
}
