package com.twitter.finagle.context

import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.{Local, Return, Throw, Try}

/**
 * A marshalled context contains bindings that may be
 * marshalled and sent across process boundaries. A set
 * of marshalled bindings may be restored in the local
 * environment. Thus we can use marshalled contexts to
 * propagate a set of bindings across a whole request
 * tree.
 */
final class MarshalledContext private[context] extends Context {

  private[this] val log = Logger.get()

  private[this] val local = new Local[Map[Buf, Cell]]

  // Cell and its sub types are package visible for testing purposes only.
  private[context] sealed trait Cell

  private[context] case class Real[A](key: Key[A], content: Some[A]) extends Cell

  private[context] case class Translucent(key: Buf, value: Buf) extends Cell {
    private[this] var cachedEnv: Some[_] = null

    // Should only be called after this Cell has been looked up by its key as the
    // key is only used to unmarshal if the parsed result hasn't been cached
    def unmarshal[A](key: Key[A]): Option[A] = {
      val result = {
        if (cachedEnv != null) cachedEnv
        else {
          key.tryUnmarshal(value) match {
            case Return(value) =>
              cachedEnv = Some(value)
              cachedEnv
            case Throw(e) =>
              val bytesToDisplay = value.slice(0, 10)
              val bytesString = Buf.slowHexString(bytesToDisplay)
              val message =
                s"Failed to deserialize marshalled context entry for key ${key.id}. " +
                  s"Value has length ${value.length}. First ${bytesToDisplay.length} bytes " +
                  s"of the value: 0x$bytesString"
              log.warning(e, message)
              None
          }
        }
      }

      result.asInstanceOf[Option[A]]
    }
  }

  /**
   * Keys in MarshalledContext must provide a marshaller
   * and unmarshaller. The key `id` is used for marshalling and
   * unmarshalling and thus must be unique. The behavior of the
   * MarshalledContext when using two keys with the same key `id`
   * is undefined.
   */
  abstract class Key[A](val id: String) {

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

  def get[A](key: Key[A]): Option[A] = env.get(key.marshalId) match {
    case Some(Real(_, someValue)) => someValue.asInstanceOf[Some[A]]
    case Some(t: Translucent) => t.unmarshal(key)
    case None => None
  }

  def let[A, R](key: Key[A], value: A)(fn: => R): R =
    letLocal(env.updated(key.marshalId, Real(key, Some(value))))(fn)

  def let[A, B, R](key1: Key[A], value1: A, key2: Key[B], value2: B)(fn: => R): R = {
    val next = env
      .updated(key1.marshalId, Real(key1, Some(value1)))
      .updated(key2.marshalId, Real(key2, Some(value2)))
    letLocal(next)(fn)
  }

  def let[R](pairs: Iterable[KeyValuePair[_]])(fn: => R): R = {
    val next = pairs.foldLeft(env) {
      case (e, KeyValuePair(k, v)) =>
        e.updated(k.marshalId, Real(k, Some(v)))
    }
    letLocal(next)(fn)
  }

  def letClear[R](key: Key[_])(fn: => R): R =
    letLocal(env - key.marshalId)(fn)

  def letClear[R](keys: Iterable[Key[_]])(fn: => R): R = {
    val next = keys.foldLeft(env) { case (e, k) => e - k.marshalId }
    letLocal(next)(fn)
  }

  def letClearAll[R](fn: => R): R = local.letClear(fn)

  /**
   * Store into the current environment a set of marshalled
   * bindings and run `fn`. Bindings are unmarshalled on demand.
   */
  def letUnmarshal[R](contexts: Iterable[(Buf, Buf)])(fn: => R): R = {
    if (contexts.isEmpty) fn
    else letLocal(doUnmarshal(env, contexts))(fn)
  }

  /**
   * Marshal the current environment into a set of (id, value) pairs.
   */
  def marshal(): Iterable[(Buf, Buf)] = marshal(env)

  // Exposed for testing
  private[context] def marshal(env: Map[Buf, Cell]): Iterable[(Buf, Buf)] = {
    env.transform {
      case (_, Real(k, v)) => k.marshal(v.get)
      case (_, Translucent(_, vBuf)) => vBuf
    }.toSeq
  }

  // Exposed for testing
  private[context] def env: Map[Buf, Cell] = local() match {
    case Some(env) => env
    case None => Map.empty
  }

  // Exposed for testing
  private[context] def doUnmarshal(
    env: Map[Buf, Cell],
    contexts: Iterable[(Buf, Buf)]
  ): Map[Buf, Cell] = {
    contexts.foldLeft(env) {
      case (env, (k, v)) =>
        env.updated(k, Translucent(k, v))
    }
  }

  // Exposed for testing
  private[context] def letLocal[T](env: Map[Buf, Cell])(fn: => T): T = local.let(env)(fn)
}
