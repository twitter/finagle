package com.twitter.finagle.context

import com.twitter.io.Buf
import com.twitter.util.{Try, Return, Throw}
import scala.collection.mutable

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
  abstract class Key[A] {
    /**
     * A unique identifier defining this marshaller. This is
     * transmitted together with marshalled values in order to 
     * pick the the appropriate unmarshaller for a given value.
     */
    def marshalId: Buf
  
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

    private def env[A](key: Key[A]): Env =
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

    def apply[A](key: Key[A]): A = env(key).apply(key)
    def get[A](key: Key[A]): Option[A] = env(key).get(key)
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
      env = Translucent(env, id, marshalled)
    }

    def build: Env = env
  }
}
