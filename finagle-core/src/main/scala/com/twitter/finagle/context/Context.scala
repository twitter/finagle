package com.twitter.finagle.context

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.io.Buf
import com.twitter.util.{Local, Try, Return, Throw}
import scala.collection.mutable

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

  sealed trait Env {
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
     * Retrieve the current definition of a key if it is defined.
     * If it is not defined, `orElse` is evaluated and returned.
     */
    def getOrElse[A](key: Key[A], orElse: () => A): A

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
    def getOrElse[A](key: Key[A], orElse: () => A): A = orElse()
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

    def getOrElse[B](key: Key[B], orElse: () => B): B =
      if (key == this.key) value.asInstanceOf[B]
      else next.getOrElse(key, orElse)

    def contains[B](key: Key[B]): Boolean =
      key == this.key || next.contains(key)

    override def toString = s"Bound($key, $value) :: $next"
  }

  /**
   * An environment without `key`. Lookups for other keys
   * are forwarded to `next`.
   */
  case class Cleared[A](next: Env, key: Key[A]) extends Env {
    def apply[B](key: Key[B]) =
      if (key == this.key) throw new NoSuchElementException
      else next(key)

    def get[B](key: Key[B])  =
      if (key == this.key) None
      else next.get(key)

    def getOrElse[B](key: Key[B], orElse: () => B): B =
      if (key == this.key) orElse()
      else next.getOrElse(key, orElse)

    def contains[B](key: Key[B]) =
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

    def getOrElse[A](key: Key[A], orElse: () => A): A =
      left.getOrElse(key, () => right.getOrElse(key, orElse))

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
   * Retrieve the current definition of a key if it is defined.
   * If it is not defined, `orElse` is evaluated and returned.
   */
  def getOrElse[A](key: Key[A], orElse: () => A): A =
    env.getOrElse(key, orElse)

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

/**
 * A marshalled context contains bindings that may be
 * marshalled and sent across process boundaries. A set
 * of marshalled bindings may be restored in the local
 * environment. Thus we can use marshalled contexts to
 * propagate a set of bindings across a whole request
 * tree.
 */
final class MarshalledContext extends Context {
  /**
   * Keys in MarshalledContext must provide a marshaller
   * and unmarshaller.
   */

  abstract class Key[A](id: String) {
    /**
     * A unique identifier defining this marshaller. This is
     * transmitted together with marshalled values in order to
     * pick the the appropriate unmarshaller for a given value.
     */
    final val marshalId: Buf = Buf.ByteBuffer.coerce(Buf.Utf8(id))

    /**
     * Marshal an A-typed value into a Buf.
     */
    def marshal(value: A): Buf

    /**
     * Attempt to unmarshal an A-typed context value.
     */
    def tryUnmarshal(buf: Buf): Try[A]
  }

  /**
   * A translucent environment is capable of storing key/value pairs
   * to be (possibly) unmarshalled later.
   */
  case class Translucent(next: Env, marshalId: Buf, marshalled: Buf) extends Env {
    @volatile private var cachedEnv: Env = null

    private def env[A](key: Key[A]): Env = {
      if (cachedEnv != null) cachedEnv
      else if (key.marshalId != marshalId) next
      else (key.tryUnmarshal(marshalled): Try[A]) match {
        case Return(value) =>
          cachedEnv = Bound(next, key, value)
          cachedEnv
        case Throw(_) =>
          // Should we omit the context altogether when this happens?
          // Should we log some warnings?
          next
      }
    }

    def apply[A](key: Key[A]): A = env(key).apply(key)
    def get[A](key: Key[A]): Option[A] = env(key).get(key)
    def getOrElse[A](key: Key[A], orElse: () => A): A = env(key).getOrElse(key, orElse)
    def contains[A](key: Key[A]): Boolean = env(key).contains(key)

    override def toString =
      if (cachedEnv != null) cachedEnv.toString else {
        val Buf.Utf8(id8) = marshalId
        s"Translucent(${id8}(${marshalled.length})) :: $next"
      }
  }

  private def marshalMap(env: Env, map: mutable.Map[Buf, Buf]): Unit =
    env match {
      case Bound(next, key, value) =>
        marshalMap(next, map)
        map.put(key.marshalId, key.marshal(value))
      case Translucent(next, id, marshalled) =>
        marshalMap(next, map)
        map.put(id, marshalled)
      case OrElse(left, right) =>
        marshalMap(right, map)
        marshalMap(left, map)
      case Cleared(next, key) =>
        marshalMap(next, map)
        map.remove(key.marshalId)
      case Empty =>
        ()
  }

  /**
   * Store into the current environment a set of marshalled
   * bindings and run `fn`. Bindings are unmarshalled on demand.
   */
  def letUnmarshal[R](contexts: Iterable[(Buf, Buf)])(fn: => R): R = {
    val u = new Unmarshaller(env)
    for ((id, marshalled) <- contexts)
      u.put(id, marshalled)
    let(u.build)(fn)
  }

  /**
   * Marshal the `env` into a set of (id, value) pairs.
   */
  def marshal(env: Env): Iterable[(Buf, Buf)] = {
    val map = mutable.Map[Buf, Buf]()
    marshalMap(env, map)
    map
  }

  /**
   * Marshal the current environment into a set of (id, value) pairs.
   */
  def marshal(): Iterable[(Buf, Buf)] =
    marshal(env)

  /**
   * Produce an environment consisting of the given marshalled
   * (id, value) pairs. They are unmarshalled on demand.
   */
  def unmarshal(contexts: Iterable[(Buf, Buf)]): Env = {
    val builder = new Unmarshaller
    for ((id, marshalled) <- contexts)
      builder.put(id, marshalled)
    builder.build
  }

  /**
   * An Unmarshaller gradually builds up an environment from
   * a set of (id, value) pairs.
   */
  class Unmarshaller(init: Env) {
    def this() = this(Empty)

    private[this] var env = init

    def put(id: Buf, marshalled: Buf) {
      // Copy the Bufs to avoid indirectly keeping a reference to Netty internal buffer (big)
      env = Translucent(env, copy(id), copy(marshalled))
    }

    def build: Env = env

    private[this] def copy(buf: Buf): Buf = buf match {
      case ChannelBufferBuf(cb) => Buf.ByteBuffer.Shared(cb.toByteBuffer)
      case _ => buf
    }
  }
}
