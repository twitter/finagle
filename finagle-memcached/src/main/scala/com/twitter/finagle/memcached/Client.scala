package com.twitter.finagle.memcached

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import _root_.java.nio.charset.StandardCharsets
import com.twitter.bijection.Bijection
import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.Bufs.{
  RichBuf,
  nonEmptyStringToBuf,
  seqOfNonEmptyStringToBuf
}
import com.twitter.finagle.partitioning.PartitionNode

import com.twitter.hashing._
import com.twitter.io.Buf
import com.twitter.util.{Command => _, Function => _, _}
import com.twitter.finagle.memcached.util.{NotFound => muNotFound}
import scala.collection.immutable

object Client {

  /**
   * Construct a client from a single Service.
   */
  def apply(raw: Service[Command, Response]): Client = {
    new ConnectedClient(raw)
  }
}

case class GetResult private[memcached] (
  hits: Map[String, Value] = Map.empty,
  misses: immutable.Set[String] = immutable.Set.empty,
  failures: Map[String, Throwable] = Map.empty) {
  lazy val values: Map[String, Buf] = hits.mapValues { _.value }.toMap

  lazy val valuesWithFlags: Map[String, (Buf, Buf)] = hits.mapValues { v =>
    v.flags match {
      case Some(x) => (v.value, x)
      case None => (v.value, Buf.Empty)
    }
  }.toMap

  def ++(o: GetResult): GetResult =
    GetResult(hits ++ o.hits, misses ++ o.misses, failures ++ o.failures)
}

case class GetsResult(getResult: GetResult) {
  def hits: Map[String, Value] = getResult.hits
  def misses: immutable.Set[String] = getResult.misses
  def failures: Map[String, Throwable] = getResult.failures
  def values: Map[String, Buf] = getResult.values
  lazy val valuesWithTokens: Map[String, (Buf, Buf)] = hits.mapValues { v =>
    (v.value, v.casUnique.get)
  }.toMap
  lazy val valuesWithFlagsAndTokens: Map[String, (Buf, Buf, Buf)] = hits.mapValues { v =>
    v.flags match {
      case Some(x) => (v.value, x, v.casUnique.get)
      case None => (v.value, Buf.Empty, v.casUnique.get)
    }
  }.toMap
  def ++(o: GetsResult): GetsResult = GetsResult(getResult ++ o.getResult)
}

object GetResult {

  private[memcached] val Empty: GetResult = GetResult()

  /**
   * Equivalent to results.reduceLeft { _ ++ _ }, but written to be more efficient.
   */
  private[memcached] def merged(results: Seq[GetResult]): GetResult = {
    results match {
      case Nil => Empty
      case Seq(single) => single
      case Seq(a, b) => a ++ b
      case _ =>
        val hits = immutable.Map.newBuilder[String, Value]
        val misses = immutable.Set.newBuilder[String]
        val failures = immutable.Map.newBuilder[String, Throwable]

        for (result <- results) {
          hits ++= result.hits
          misses ++= result.misses
          failures ++= result.failures
        }

        GetResult(hits.result(), misses.result(), failures.result())
    }
  }

  private[memcached] def merged(results: Seq[GetsResult]): GetsResult = {
    val unwrapped = results.map { _.getResult }
    GetsResult(merged(unwrapped))
  }
}

/**
 * The result of a check and set command.
 *
 * @see [[BaseClient.checkAndSet]]
 */
sealed trait CasResult {

  /**
   * Whether or not the operation replaced the value.
   *
   * This may be useful for developers transitioning to the
   * [[Client.checkAndSet]] methods from the deprecated
   * [[Client.cas]] methods.
   */
  def replaced: Boolean
}

object CasResult {
  case object Stored extends CasResult {
    def replaced: Boolean = true
  }
  case object Exists extends CasResult {
    def replaced: Boolean = false
  }
  case object NotFound extends CasResult {
    def replaced: Boolean = false
  }
}

private object BaseClient {
  private[this] val GetFn: Map[String, Any] => Option[Any] =
    map => map.values.headOption

  def getFn[T]: Map[String, T] => Option[T] =
    GetFn.asInstanceOf[Map[String, T] => Option[T]]

  def getsFn[T]: Map[String, (T, Buf)] => Option[(T, Buf)] =
    GetFn.asInstanceOf[Map[String, (T, Buf)] => Option[(T, Buf)]]

  def getWithFlagFn[T]: Map[String, (T, Buf)] => Option[(T, Buf)] =
    GetFn.asInstanceOf[Map[String, (T, Buf)] => Option[(T, Buf)]]

  def getsWithFlagFn[T]: Map[String, (T, Buf, Buf)] => Option[(T, Buf, Buf)] =
    GetFn.asInstanceOf[Map[String, (T, Buf, Buf)] => Option[(T, Buf, Buf)]]
}

/**
 * A friendly client to talk to a Memcached server.
 *
 * @see The Memcached
 *      [[https://github.com/memcached/memcached/blob/master/doc/protocol.txt protocol docs]]
 *      for details on the API.
 * @define flags `flags` is an arbitrary integer that the server stores along with
 *               the data and sends back when the item is retrieved.
 *               Clients may use this as a bit field to store data-specific
 *               information; this field is opaque to the server.
 * @define expiry `expiry` is the expiration time for entries. If it is Time.epoch`,
 *                `Time.Top`, `Time.Bottom` or `Time.Undefined` then the item
 *                never expires, although it may be deleted from the cache to
 *                make room for other items. This is also the case for values
 *                where the number of seconds is larger than `Long.MaxValue`.
 *                Otherwise, clients will not be able to retrieve this item after
 *                the expiration time arrives (measured on the cache server).
 *
 */
trait BaseClient[T] extends Closable {
  import BaseClient._

  /**
   * Deserialize from the bytes in a `Buf` into the client's type, `T`.
   */
  def bufferToType(a: Buf): T

  /**
   * Store a key. Override an existing value.
   *
   * $flags
   *
   * $expiry
   */
  def set(key: String, flags: Int, expiry: Time, value: T): Future[Unit]

  /**
   * Store a key but only if it doesn't already exist on the server.
   *
   * $flags
   *
   * $expiry
   *
   * @return true if stored, false if not stored
   */
  def add(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean]

  /**
   * Append bytes to the end of an existing key. If the key doesn't exist, the
   * operation has no effect.
   *
   * $flags
   *
   * $expiry
   *
   * @return true if stored, false if not stored
   */
  def append(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean]

  /**
   * Prepend bytes to the beginning of an existing key. If the key doesn't
   * exist, the operation has no effect.
   *
   * $flags
   *
   * $expiry
   *
   * @return true if stored, false if not stored
   */
  def prepend(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean]

  /**
   * Replace bytes on an existing key. If the key doesn't exist, the
   * operation has no effect.
   *
   * $flags
   *
   * $expiry
   *
   * @return true if stored, false if not stored
   */
  def replace(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean]

  /**
   * Perform a CAS operation on the key, only if the value has not
   * changed since the value was last retrieved, and `casUnique`
   * extracted from a `gets` command.  We treat the "cas unique" token
   * opaquely, but in reality it is a string-encoded u64.
   *
   * $flags
   *
   * $expiry
   *
   * @return [[Stored]] if the operation was successful, [[Exists]] if the
   *        operation failed because someone else had changed the value,
   *        or [[NotFound]] if the key was not found in the cache.
   * @see [[gets]] and [[getsResult]] for retreiving the cas token.
   */
  def checkAndSet(
    key: String,
    flags: Int,
    expiry: Time,
    value: T,
    casUnique: Buf
  ): Future[CasResult]

  /**
   * Perform a CAS operation on the key, only if the value has not changed
   * since the value was last retrieved.  This is enforced by passing a
   * `casUnique` token extracted from a `gets` command.  If the `casUnique`
   * token matches the one on the server, the value is replaced.  We treat the
   * "cas unique" token opaquely, but in reality it is a string-encoded u64.
   *
   * Neither flags nor expiry are supplied.
   *
   * @return [[Stored]] if the operation was successful, [[Exists]] if the
   *        operation failed because someone else had changed the value,
   *        or [[NotFound]] if the key was not found in the cache.
   * @see [[gets]] and [[getsResult]] for retreiving the cas token.
   */
  def checkAndSet(key: String, value: T, casUnique: Buf): Future[CasResult] =
    checkAndSet(key, 0, Time.epoch, value, casUnique)

  /**
   * Get a key from the server.
   *
   * @return `None` if there is no value stored for `key`.
   * @see [[gets]] if you need a "cas unique" token.
   */
  def get(key: String): Future[Option[T]] =
    get(Seq(key)).map(getFn)

  /**
   * Get a key from the server along with a "cas unique" token.  The token
   * is treated opaquely by the memcache client but is in reality a
   * string-encoded u64.
   *
   * @return `None` if there is no value stored for `key`.
   * @see [[get]] if you do not need a "cas unique" token.
   * @see [[checkAndSet]] for using the token.
   */
  def gets(key: String): Future[Option[(T, Buf)]] =
    gets(Seq(key)).map(getsFn)

  /**
   * Get a key from the server along with its flags
   *
   * @return `None` if there is no value stored for `key`.
   * @see [[get]] if you do not need the flags.
   */
  def getWithFlag(key: String): Future[Option[(T, Buf)]] =
    getWithFlag(Seq(key)).map(getWithFlagFn)

  /**
   * Get a key from the server along with its flags and a "cas unique" token.
   *
   * @return `None` if there is no value stored for `key`.
   * @see [[get]] if you do not need the flags or the token.
   * @see [[gets]] if you do not need the flags.
   * @see [[getWithFlag]] if you do not need the token.
   */
  def getsWithFlag(key: String): Future[Option[(T, Buf, Buf)]] =
    getsWithFlag(Seq(key)).map(getsWithFlagFn)

  /**
   * Get a set of keys from the server.
   *
   * @return a Map[String, T] of all of the keys that the server had.
   * @see [[gets]] if you need a "cas unique" token.
   */
  def get(keys: Iterable[String]): Future[Map[String, T]] = {
    getResult(keys).flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.values.mapValues(bufferToType).toMap)
      }
    }
  }

  /**
   * Get a set of keys from the server, together with a "cas unique"
   * token.  The token is treated opaquely by the memcache client but
   * is in reality a string-encoded u64.
   *
   * @return a Map[String, (T, Buf)] of all the
   * keys the server had, together with their "cas unique" token
   * @see [[get]] if you do not need a "cas unique" token.
   * @see [[checkAndSet]] for using the token.
   */
  def gets(keys: Iterable[String]): Future[Map[String, (T, Buf)]] = {
    getsResult(keys) flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.valuesWithTokens.mapValues {
          case (v, u) => (bufferToType(v), u)
        }.toMap)
      }
    }
  }

  /**
   * Get a set of keys from the server, together with their flags
   *
   * @return a Map[String, (T, Buf)] of all the
   * keys the server had, together with their flags
   * @see [[get]] if you do not need the flags
   */
  def getWithFlag(keys: Iterable[String]): Future[Map[String, (T, Buf)]] = {
    getResult(keys) flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.valuesWithFlags.mapValues {
          case (v, u) => (bufferToType(v), u)
        }.toMap)
      }
    }
  }

  /**
   * Get a set of keys from the server, together with a "cas unique"
   * token and flags. The token is treated opaquely by the memcache
   * client but is in reality a string-encoded u64.
   *
   * @return a Map[String, (T, Buf, Buf)] of all the keys
   * the server had, together with their "cas unique" token and flags
   * @see [[get]] if you do not need the token or the flags.
   * @see [[gets]] if you do not need the flag.
   * @see [[getWithFlag]] if you do not need the token.
   * @see [[checkAndSet]] for using the token.
   */
  def getsWithFlag(keys: Iterable[String]): Future[Map[String, (T, Buf, Buf)]] = {
    getsResult(keys) flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.valuesWithFlagsAndTokens.mapValues {
          case (v, u, t) => (bufferToType(v), u, t)
        }.toMap)
      }
    }
  }

  /**
   * Get a set of keys from the server. Returns a Future[GetResult] that
   * encapsulates hits, misses and failures.
   *
   * @see [[getsResult]] if you need "cas unique" tokens.
   */
  def getResult(keys: Iterable[String]): Future[GetResult]

  /**
   * Get a set of keys from the server. Returns a Future[GetsResult] that
   * encapsulates hits, misses and failures. This variant includes the casToken
   * from memcached.
   *
   * @see [[getResult]] if you do not need "cas unique" tokens.
   * @see [[checkAndSet]] for using the token.
   */
  def getsResult(keys: Iterable[String]): Future[GetsResult]

  /**
   * Remove a key.
   *
   * @return true if deleted, false if not found
   */
  def delete(key: String): Future[JBoolean]

  /**
   * Increment the `key` by `delta`.
   *
   * Interprets the stored value for `key` as a Long if it is parseable
   * as a decimal representation of a 64-bit unsigned integer.
   *
   * This operation has no effect if there is no value there already.
   */
  def incr(key: String, delta: Long): Future[Option[JLong]]

  /**
   * Increment the `key` by `1`.
   *
   * Interprets the stored value for `key` as a Long if it is parseable
   * as a decimal representation of a 64-bit unsigned integer.
   *
   * This operation has no effect if there is no value there already.
   */
  def incr(key: String): Future[Option[JLong]] = incr(key, 1L)

  /**
   * Decrement the `key` by `n`.
   *
   * Interprets the stored value for `key` as a Long if it is parseable
   * as a decimal representation of a 64-bit unsigned integer.
   *
   * This operation has no effect if there is no value there already.
   */
  def decr(key: String, delta: Long): Future[Option[JLong]]

  /**
   * Decrement the `key` by 1.
   *
   * Interprets the stored value for `key` as a Long if it is parseable
   * as a decimal representation of a 64-bit unsigned integer.
   *
   * This operation has no effect if there is no value there already.
   */
  def decr(key: String): Future[Option[JLong]] = decr(key, 1L)

  /**
   * Store a key. Override an existing values.
   *
   * Neither flags nor expiry are supplied.
   */
  def set(key: String, value: T): Future[Unit] =
    set(key, 0, Time.epoch, value)

  /**
   * Store a key but only if it doesn't already exist on the server.
   *
   * Neither flags nor expiry are supplied.
   *
   * @return true if stored, false if not stored
   */
  def add(key: String, value: T): Future[JBoolean] =
    add(key, 0, Time.epoch, value)

  /**
   * Append a set of bytes to the end of an existing key. If the key doesn't
   * exist, the operation has no effect.
   *
   * @return true if stored, false if not stored
   */
  def append(key: String, value: T): Future[JBoolean] =
    append(key, 0, Time.epoch, value)

  /**
   * Prepend a set of bytes to the beginning of an existing key. If the key
   * doesn't exist, the operation has no effect.
   *
   * Neither flags nor expiry are supplied.
   *
   * @return true if stored, false if not stored
   */
  def prepend(key: String, value: T): Future[JBoolean] =
    prepend(key, 0, Time.epoch, value)

  /**
   * Replace an item if it exists. If it doesn't exist, the operation has no
   * effect.
   *
   * Neither flags nor expiry are supplied.
   *
   * @return true if stored, false if not stored
   */
  def replace(key: String, value: T): Future[JBoolean] = replace(key, 0, Time.epoch, value)

  /**
   * Send a quit command to the server. Alternative to release, for
   * protocol compatibility.
   */
  def quit(): Future[Unit] = close()

  /**
   * Send a stats command with optional arguments to the server.
   *
   * @return a sequence of strings, each of which is a line of output
   */
  def stats(args: Option[String]): Future[Seq[String]]

  /**
   * Send a stats command with the given `args` to the server.
   *
   * @return a sequence of strings, each of which is a line of output
   */
  def stats(args: String): Future[Seq[String]] = stats(Some(args))

  /**
   * Send a stats command to the server.
   *
   * @return a sequence of strings, each of which is a line of output
   */
  def stats(): Future[Seq[String]] = stats(None)

  /**
   * Close the underlying resources.
   */
  def close(deadline: Time): Future[Unit]

}

trait Client extends BaseClient[Buf] {
  def bufferToType(v: Buf): Buf = v

  def adapt[T](bijection: Bijection[Buf, T]): BaseClient[T] =
    new ClientAdaptor[T](this, bijection)

  /** Adaptor to use String as values */
  def withStrings: BaseClient[String] = adapt(
    new Bijection[Buf, String] {
      def apply(a: Buf): String = a match { case Buf.Utf8(s) => s }
      override def invert(b: String): Buf = Buf.Utf8(b)
    }
  )

  /** Adaptor to use Array[Byte] as values */
  def withBytes: BaseClient[Array[Byte]] = adapt(
    new Bijection[Buf, Array[Byte]] {
      def apply(a: Buf): Array[Byte] = a.toArray
      override def invert(b: Array[Byte]): Buf = Buf.ByteArray.Owned(b)
    }
  )
}

trait ProxyClient extends Client {
  protected def proxyClient: Client

  def getResult(keys: Iterable[String]): Future[GetResult] = proxyClient.getResult(keys)

  def getsResult(keys: Iterable[String]): Future[GetsResult] = proxyClient.getsResult(keys)

  def set(key: String, flags: Int, expiry: Time, value: Buf): Future[Unit] =
    proxyClient.set(key, flags, expiry, value)

  def add(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    proxyClient.add(key, flags, expiry, value)

  def replace(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    proxyClient.replace(key, flags, expiry, value)

  def append(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    proxyClient.append(key, flags, expiry, value)

  def prepend(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    proxyClient.prepend(key, flags, expiry, value)

  def incr(key: String, delta: Long): Future[Option[JLong]] = proxyClient.incr(key, delta)

  def decr(key: String, delta: Long): Future[Option[JLong]] = proxyClient.decr(key, delta)

  def checkAndSet(
    key: String,
    flags: Int,
    expiry: Time,
    value: Buf,
    casUnique: Buf
  ): Future[CasResult] =
    proxyClient.checkAndSet(key, flags, expiry, value, casUnique)

  def delete(key: String): Future[JBoolean] = proxyClient.delete(key)

  def stats(args: Option[String]): Future[Seq[String]] = proxyClient.stats(args)

  def close(deadline: Time): Future[Unit] = proxyClient.close(deadline)
}

private[memcached] object ClientConstants {
  val JavaTrue: Future[JBoolean] = Future.value(true)
  val JavaFalse: Future[JBoolean] = Future.value(false)

  val FutureExists: Future[CasResult] = Future.value(CasResult.Exists)
  val FutureNotFound: Future[CasResult] = Future.value(CasResult.NotFound)
  val FutureStored: Future[CasResult] = Future.value(CasResult.Stored)

  val CasFromCheckAndSet: CasResult => Future[JBoolean] = {
    case CasResult.Stored => JavaTrue
    case CasResult.Exists => JavaFalse
    case CasResult.NotFound => JavaFalse
  }

  def hitsFromValues(values: Seq[Value]): Map[String, Value] =
    values.iterator.map { value =>
      val Buf.Utf8(keyStr) = value.key
      (keyStr, value)
    }.toMap
}

/**
 * A Client connected to an individual Memcached server.
 *
 * @param  service  the underlying Memcached Service.
 */
protected class ConnectedClient(protected val service: Service[Command, Response]) extends Client {
  import ClientConstants._

  protected def rawGet(command: RetrievalCommand): Future[GetResult] = {
    val keys: immutable.Set[String] = command.keys.iterator.map { case Buf.Utf8(s) => s }.toSet

    service(command).transform {
      case Return(Values(values)) =>
        val hits: Map[String, Value] = hitsFromValues(values)
        val misses = muNotFound(keys, hits.keySet)
        Future.value(GetResult(hits, misses))
      case Return(Error(e)) => throw e
      case Return(ValuesAndErrors(values, errors)) =>
        val hits: Map[String, Value] = hitsFromValues(values)
        val failures = errors.map {
          case (buf, e) =>
            val Buf.Utf8(keyStr) = buf
            (keyStr, e)
        }
        val misses = muNotFound(keys, hits.keySet ++ failures.keySet)
        Future.value(GetResult(hits, misses, failures))
      case Return(other) =>
        throw new IllegalStateException(
          "Invalid response type from get: %s".format(other.getClass.getSimpleName)
        )
      case Throw(t: RequestException) =>
        Future.value(GetResult(failures = keys.iterator.map { (_, t) }.toMap))
      case Throw(t: ChannelException) =>
        Future.value(GetResult(failures = keys.iterator.map { (_, t) }.toMap))
      case Throw(t: ServiceException) =>
        Future.value(GetResult(failures = keys.iterator.map { (_, t) }.toMap))
      case t => Future.const(t.asInstanceOf[Try[GetResult]])
    }
  }

  def getResult(keys: Iterable[String]): Future[GetResult] = {
    try {
      if (keys == null) throw new IllegalArgumentException("Invalid keys: keys cannot be null")
      rawGet(Get(keys))
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For keys: " + keys))
    }
  }
  def getsResult(keys: Iterable[String]): Future[GetsResult] = {
    try {
      if (keys == null) throw new IllegalArgumentException("Invalid keys: keys cannot be null")
      rawGet(Gets(keys)).map { GetsResult(_) }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For keys: " + keys))
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: Buf): Future[Unit] = {
    try {
      service(Set(key, flags, expiry, value)).map {
        case Stored => ()
        case Error(e) => throw e
        case response => throw new IllegalStateException(s"Invalid response: $response")
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def checkAndSet(
    key: String,
    flags: Int,
    expiry: Time,
    value: Buf,
    casUnique: Buf
  ): Future[CasResult] = {
    try {
      service(Cas(key, flags, expiry, value, casUnique)).flatMap {
        case Stored => FutureStored
        case Exists => FutureExists
        case NotFound => FutureNotFound
        case Error(e) => Future.exception(e)
        case _ => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def add(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] = {
    try {
      service(Add(key, flags, expiry, value)).flatMap {
        case Stored => JavaTrue
        case NotStored => JavaFalse
        case Error(e) => Future.exception(e)
        case _ => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def append(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] = {
    try {
      service(Append(key, flags, expiry, value)).flatMap {
        case Stored => JavaTrue
        case NotStored => JavaFalse
        case Error(e) => Future.exception(e)
        case _ => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def prepend(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] = {
    try {
      service(Prepend(key, flags, expiry, value)).flatMap {
        case Stored => JavaTrue
        case NotStored => JavaFalse
        case Error(e) => Future.exception(e)
        case _ => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def replace(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] = {
    try {
      service(Replace(key, flags, expiry, value)).flatMap {
        case Stored => JavaTrue
        case NotStored => JavaFalse
        case Error(e) => Future.exception(e)
        case _ => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def delete(key: String): Future[JBoolean] = {
    try {
      service(Delete(key)).flatMap {
        case Deleted => JavaTrue
        case NotFound => JavaFalse
        case Error(e) => Future.exception(e)
        case _ => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def incr(key: String, delta: Long): Future[Option[JLong]] = {
    try {
      service(Incr(key, delta)).flatMap {
        case Number(value) => Future.value(Some(value))
        case NotFound => Future.None
        case Error(e) => Future.exception(e)
        case _ => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def decr(key: String, delta: Long): Future[Option[JLong]] = {
    try {
      service(Decr(key, delta)).flatMap {
        case Number(value) => Future.value(Some(value))
        case NotFound => Future.None
        case Error(e) => Future.exception(e)
        case _ => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException =>
        Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def stats(args: Option[String]): Future[Seq[String]] = {
    val statArgs: Seq[Buf] = args match {
      case None => Seq(Buf.Empty)
      case Some(args) => args.split(" ").iterator.map(nonEmptyStringToBuf).toSeq
    }
    service(Stats(statArgs)).flatMap {
      case InfoLines(lines) =>
        Future {
          lines.map { line =>
            val key = line.key
            val values = line.values
            val Buf.Utf8(keyStr) = key
            "%s %s".format(keyStr, values.map { case Buf.Utf8(str) => str }.mkString(" "))
          }
        }
      case Error(e) => Future.exception(e)
      case Values(list) => Future.Nil
      case _ => Future.exception(new IllegalStateException)
    }
  }

  def close(deadline: Time): Future[Unit] =
    service.close()

}

/**
 * A partitioned client is a client that delegates to an actual client based on
 * the key value.  Subclasses implement clientOf to choose the Client.
 */
trait PartitionedClient extends Client {
  protected[memcached] def clientOf(key: String): Client

  private[this] def withKeysGroupedByClient[A](
    keys: Iterable[String]
  )(
    f: (Client, Iterable[String]) => Future[A]
  ): Future[Seq[A]] = {
    Future.collect(
      keys.groupBy(clientOf).iterator.map(Function.tupled(f)).toSeq
    )
  }

  def getResult(keys: Iterable[String]): Future[GetResult] = {
    if (keys.nonEmpty) {
      withKeysGroupedByClient(keys) {
        _.getResult(_)
      }.map { GetResult.merged(_) }
    } else {
      Future.value(GetResult.Empty)
    }
  }

  def getsResult(keys: Iterable[String]): Future[GetsResult] = {
    if (keys.nonEmpty) {
      withKeysGroupedByClient(keys) {
        _.getsResult(_)
      }.map { GetResult.merged(_) }
    } else {
      Future.value(GetsResult(GetResult.Empty))
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: Buf): Future[Unit] =
    clientOf(key).set(key, flags, expiry, value)
  def add(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    clientOf(key).add(key, flags, expiry, value)
  def append(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    clientOf(key).append(key, flags, expiry, value)
  def prepend(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    clientOf(key).prepend(key, flags, expiry, value)
  def replace(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    clientOf(key).replace(key, flags, expiry, value)
  def checkAndSet(
    key: String,
    flags: Int,
    expiry: Time,
    value: Buf,
    casUnique: Buf
  ): Future[CasResult] =
    clientOf(key).checkAndSet(key, flags, expiry, value, casUnique)

  def delete(key: String): Future[JBoolean] = clientOf(key).delete(key)
  def incr(key: String, delta: Long): Future[Option[JLong]] = clientOf(key).incr(key, delta)
  def decr(key: String, delta: Long): Future[Option[JLong]] = clientOf(key).decr(key, delta)

  def stats(args: Option[String]): Future[Seq[String]] =
    throw new UnsupportedOperationException("No logical way to perform stats without a key")
}

/**
 * Ruby memcache-client (MemCache) compatible client.
 */
class RubyMemCacheClient(clients: Seq[Client]) extends PartitionedClient {
  protected[memcached] def clientOf(key: String): Client = {
    val bytes = key.getBytes(StandardCharsets.UTF_8)
    val hash = (KeyHasher.CRC32_ITU.hashKey(bytes) >> 16) & 0x7fff
    val index = hash % clients.size
    clients(index.toInt)
  }

  def close(deadline: Time): Future[Unit] =
    Closables.all(clients: _*).close(deadline)

}

/**
 * Builder for memcache-client (MemCache) compatible client.
 */
case class RubyMemCacheClientBuilder(
  _nodes: Seq[(String, Int, Int)],
  _clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]]) {

  def this() = this(
    Nil, // nodes
    None // clientBuilder
  )

  def nodes(nodes: Seq[(String, Int, Int)]): RubyMemCacheClientBuilder =
    copy(_nodes = nodes)

  def nodes(hostPortWeights: String): RubyMemCacheClientBuilder =
    copy(_nodes = CacheNodeGroup(hostPortWeights).members.iterator.map { node: PartitionNode =>
      (node.host, node.port, node.weight)
    }.toSeq)

  def clientBuilder(
    clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]
  ): RubyMemCacheClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def build(): PartitionedClient = {
    val builder = _clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1).daemon(true)
    val clients = _nodes.map {
      case (hostname, port, weight) =>
        require(weight == 1, "Ruby memcache node weight must be 1")
        Client(Memcached.client.newService(hostname + ":" + port))
    }
    new RubyMemCacheClient(clients)
  }
}

/**
 * PHP memcache-client (memcache.so) compatible client.
 */
class PHPMemCacheClient(clients: Array[Client], keyHasher: KeyHasher) extends PartitionedClient {
  protected[memcached] def clientOf(key: String): Client = {
    // See mmc_hash() in memcache_standard_hash.c
    val hash = (keyHasher.hashKey(key.getBytes) >> 16) & 0x7fff
    val index = hash % clients.length
    clients(index.toInt)
  }

  def close(deadline: Time): Future[Unit] =
    Closable.all(clients: _*).close(deadline)

}

/**
 * Builder for memcache-client (memcache.so) compatible client.
 */
case class PHPMemCacheClientBuilder(
  _nodes: Seq[(String, Int, Int)],
  _hashName: Option[String],
  _clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]]) {

  def nodes(nodes: Seq[(String, Int, Int)]): PHPMemCacheClientBuilder =
    copy(_nodes = nodes)

  def nodes(hostPortWeights: String): PHPMemCacheClientBuilder =
    copy(_nodes = CacheNodeGroup(hostPortWeights).members.iterator.map { node: PartitionNode =>
      (node.host, node.port, node.weight)
    }.toSeq)

  def hashName(hashName: String): PHPMemCacheClientBuilder =
    copy(_hashName = Some(hashName))

  def clientBuilder(
    clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]
  ): PHPMemCacheClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def build(): PartitionedClient = {
    val builder = _clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1).daemon(true)
    val keyHasher = KeyHasher.byName(_hashName.getOrElse("crc32-itu"))
    val clients = _nodes.flatMap {
      case (hostname, port, weight) =>
        val client = Client(Memcached.client.newService(hostname + ":" + port))
        for (i <- (1 to weight)) yield client
    }.toArray
    new PHPMemCacheClient(clients, keyHasher)
  }
}

object PHPMemCacheClientBuilder {
  def apply(): PHPMemCacheClientBuilder = PHPMemCacheClientBuilder(Nil, Some("crc32-itu"), None)
  def get(): PHPMemCacheClientBuilder = apply()
}
