package com.twitter.finagle.context

import com.twitter.finagle.CoreToggles
import com.twitter.finagle.server.ServerInfo
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.{Local, Return, Throw, Try}
import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.util.Locale

private object MarshalledContext {
  val toggle = CoreToggles("com.twitter.finagle.context.MarshalledContextLookupId")

  def caseInsensitiveLookupId(id: String): String = id.toLowerCase(Locale.US)
}

/**
 * A marshalled context contains bindings that may be
 * marshalled and sent across process boundaries. A set
 * of marshalled bindings may be restored in the local
 * environment. Thus we can use marshalled contexts to
 * propagate a set of bindings across a whole request
 * tree.
 */
final class MarshalledContext private[context] extends Context {
  import MarshalledContext._

  private[this] val useCaseInsensitiveLookup: Boolean =
    if (toggle.isDefined) toggle(ServerInfo().id.hashCode)
    else true

  private[this] val log = Logger.get()

  private[this] val local = new Local[Map[String, Cell]]

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
              val message =
                s"Failed to deserialize marshalled context entry for key ${key.id}. " +
                  s"Value has length ${value.length}. Values hash: 0x${hashValue(value)}"
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
   * and unmarshaller. The `key` is used for marshalling and
   * unmarshalling and thus must be unique. The behavior of the
   * MarshalledContext when using two keys with the same key `id`
   * is undefined.
   *
   * @Note the key's `id` is treated without case sensitivity.
   */
  abstract class Key[A](val id: String) {

    /**
     * A unique identifier defining this marshaller. This is
     * transmitted together with marshalled values in order to
     * pick the the appropriate unmarshaller for a given value.
     */
    final val marshalId: Buf = Buf.ByteBuffer.coerce(Buf.Utf8(id))

    /**
     * The identifier used to lookup the key in the stored context.
     */
    private[context] final val lookupId: String = {
      if (useCaseInsensitiveLookup) MarshalledContext.caseInsensitiveLookupId(id)
      else id
    }

    /**
     * Marshal an A-typed value into a Buf.
     */
    def marshal(value: A): Buf

    /**
     * Attempt to unmarshal an A-typed context value.
     */
    def tryUnmarshal(buf: Buf): Try[A]
  }

  def get[A](key: Key[A]): Option[A] = env.get(key.lookupId) match {
    case Some(Real(_, someValue)) => someValue.asInstanceOf[Some[A]]
    case Some(t: Translucent) => t.unmarshal(key)
    case None => None
  }

  def let[A, R](key: Key[A], value: A)(fn: => R): R =
    letLocal(env.updated(key.lookupId, Real(key, Some(value))))(fn)

  def let[A, B, R](key1: Key[A], value1: A, key2: Key[B], value2: B)(fn: => R): R = {
    val next = env
      .updated(key1.lookupId, Real(key1, Some(value1)))
      .updated(key2.lookupId, Real(key2, Some(value2)))
    letLocal(next)(fn)
  }

  def let[R](pairs: Iterable[KeyValuePair[_]])(fn: => R): R = {
    val next = pairs.foldLeft(env) {
      case (e, KeyValuePair(k, v)) =>
        e.updated(k.lookupId, Real(k, Some(v)))
    }
    letLocal(next)(fn)
  }

  def letClear[R](key: Key[_])(fn: => R): R =
    letLocal(env - key.lookupId)(fn)

  def letClear[R](keys: Iterable[Key[_]])(fn: => R): R = {
    val next = keys.foldLeft(env) { case (e, k) => e - k.lookupId }
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
  private[context] def marshal(env: Map[String, Cell]): Iterable[(Buf, Buf)] = {
    // Since we internally store our keys in a case insensitive format, we want to
    // make sure we marshal our keys using the original `marshalId` which preserves
    // the casing of the context key.
    env.map {
      case (_, Real(k, v)) => (k.marshalId, k.marshal(v.get))
      case (_, Translucent(k, vBuf)) => (k, vBuf)
    }
  }

  // Exposed for testing
  private[context] def env: Map[String, Cell] = local() match {
    case Some(env) => env
    case None => Map.empty
  }

  // Exposed for testing
  private[context] def doUnmarshal(
    env: Map[String, Cell],
    contexts: Iterable[(Buf, Buf)]
  ): Map[String, Cell] = {
    contexts.foldLeft(env) {
      case (env, (k @ Buf.Utf8(id), v)) =>
        if (useCaseInsensitiveLookup) env.updated(caseInsensitiveLookupId(id), Translucent(k, v))
        else env.updated(id, Translucent(k, v))
    }
  }

  // Exposed for testing
  private[context] def hashValue(value: Buf): String = {
    // SHA-256 is considered a cryptographically secure hash function as of 2021.
    val algorithm = "SHA-256"
    try {
      val digest = MessageDigest.getInstance(algorithm)
      value match {
        case Buf.ByteArray.Owned(data, start, end) =>
          digest.update(data, start, end - start)
        case other =>
          val data = Buf.ByteArray.Owned.extract(other)
          digest.update(data)
      }

      Buf.slowHexString(Buf.ByteArray.Owned(digest.digest()))
    } catch {
      case _: NoSuchAlgorithmException => s"<hash algorithm $algorithm unavailable>"
    }
  }

  // Exposed for testing
  private[context] def letLocal[T](env: Map[String, Cell])(fn: => T): T = local.let(env)(fn)
}
