package com.twitter.finagle.memcached

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import _root_.java.net.{InetSocketAddress, SocketAddress}
import com.twitter.bijection.Bijection
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig, Cluster}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.memcached.exp.LocalMemcached
import com.twitter.finagle.memcached.protocol.{text, _}
import com.twitter.finagle.memcached.util.Bufs.{RichBuf, nonEmptyStringToBuf, seqOfNonEmptyStringToBuf}
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.finagle.service.{FailedService, ReqRep, ResponseClassifier, FailureAccrualFactory}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.hashing._
import com.twitter.io.{Buf, Charsets}
import com.twitter.logging.Level
import com.twitter.util.{Command => _, Function => _, _}
import scala.collection.breakOut
import scala.collection.{immutable, mutable}


object Client {
  /**
   * Construct a client from a single host.
   *
   * @param host a String of host:port combination.
   */
  def apply(host: String): Client = Client(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(text.Memcached())
      .daemon(true)
      .build())

  /**
   * Construct a client from a Name
   */
  def apply(name: Name): Client = Client(
    ClientBuilder()
      .dest(name)
      .hostConnectionLimit(1)
      .codec(new text.Memcached)
      .daemon(true)
      .build())

  /**
   * Construct a client from a Group
   */
  @deprecated("Use `apply(name: Name)` instead", "7.0.0")
  def apply(group: Group[SocketAddress]): Client = Client(
    ClientBuilder()
      .group(group)
      .hostConnectionLimit(1)
      .codec(new text.Memcached)
      .daemon(true)
      .build())

  /**
   * Construct a client from a Cluster
   */
  @deprecated("Use `apply(name: Name)` instead", "7.0.0")
  def apply(cluster: Cluster[SocketAddress]): Client = Client(
    ClientBuilder()
      .cluster(cluster)
      .hostConnectionLimit(1)
      .codec(new text.Memcached)
      .daemon(true)
      .build())

  /**
   * Construct a client from a single Service.
   */
  def apply(raw: Service[Command, Response]): Client = {
    new ConnectedClient(raw)
  }
}

case class GetResult private[memcached](
  hits: Map[String, Value] = Map.empty,
  misses: immutable.Set[String] = immutable.Set.empty,
  failures: Map[String, Throwable] = Map.empty
) {
  lazy val values: Map[String, Buf] = hits.mapValues { _.value }

  def ++(o: GetResult): GetResult =
    GetResult(hits ++ o.hits, misses ++ o.misses, failures ++ o.failures)
}

case class GetsResult(getResult: GetResult) {
  def hits = getResult.hits
  def misses = getResult.misses
  def failures = getResult.failures
  def values = getResult.values
  lazy val valuesWithTokens = hits.mapValues { v => (v.value, v.casUnique.get) }
  def ++(o: GetsResult) = GetsResult(getResult ++ o.getResult)
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

sealed trait CasResult
object CasResult {
  case object Stored extends CasResult
  case object Exists extends CasResult
  case object NotFound extends CasResult
}

/**
 * A friendly client to talk to a Memcached server.
 */
trait BaseClient[T] {
  import ClientConstants._
  def bufferToType(a: Buf): T

  /**
   * Store a key. Override an existing value.
   * @return true
   */
  def set(key: String, flags: Int, expiry: Time, value: T): Future[Unit]

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  def add(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean]

  /**
   * Append bytes to the end of an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  def append(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean]

  /**
   * Prepend bytes to the beginning of an existing key. If the key doesn't
   * exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def prepend(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean]

  /**
   * Replace bytes on an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  def replace(key: String, flags: Int, expiry: Time, value: T): Future[JBoolean]

  /**
   * Perform a CAS operation on the key, only if the value has not
   * changed since the value was last retrieved, and `casUnique`
   * extracted from a `gets` command.  We treat the "cas unique" token
   * opaquely, but in reality it is a string-encoded u64.
   *
   * @return true if replaced, false if not
   * @note this is superceded by `checkAndSet` which returns a higher fidelity
   *       return value
   */
  @deprecated("BaseClient.cas deprecated in favor of checkAndSet", "2015-12-10")
  final def cas(
    key: String, flags: Int, expiry: Time, value: T, casUnique: Buf
  ): Future[JBoolean] =
    checkAndSet(key, flags, expiry, value, casUnique).flatMap(CasFromCheckAndSet)

  /**
   * Perform a CAS operation on the key, only if the value has not
   * changed since the value was last retrieved, and `casUnique`
   * extracted from a `gets` command.  We treat the "cas unique" token
   * opaquely, but in reality it is a string-encoded u64.
   *
   * @return true if replaced, false if not
   * @note this is superceded by `checkAndSet` which returns a higher fidelity
   *       return value
   */
  @deprecated("BaseClient.cas deprecated in favor of checkAndSet", "2015-12-10")
  final def cas(key: String, value: T, casUnique: Buf): Future[JBoolean] =
    cas(key, 0, Time.epoch, value, casUnique)

  /**
   * Perform a CAS operation on the key, only if the value has not
   * changed since the value was last retrieved, and `casUnique`
   * extracted from a `gets` command.  We treat the "cas unique" token
   * opaquely, but in reality it is a string-encoded u64.
   *
   * @return [[Stored]] if the operation was successful, [[Exists]] if the
   *        operation failed because someone else had changed the value,
   *        or [[NotFound]] if the key was not found in the cache.
   */
  def checkAndSet(
    key: String, flags: Int, expiry: Time, value: T, casUnique: Buf
  ): Future[CasResult]

  /**
   * Perform a CAS operation on the key, only if the value has not changed
   * since the value was last retrieved.  This is enforced by passing a
   * `casUnique` token extracted from a `gets` command.  If the `casUnique`
   * token matches the one on the server, the value is replaced.  We treat the
   * "cas unique" token opaquely, but in reality it is a string-encoded u64.
   *
   * @return [[Stored]] if the operation was successful, [[Exists]] if the
   *        operation failed because someone else had changed the value,
   *        or [[NotFound]] if the key was not found in the cache.
   */
  def checkAndSet(key: String, value: T, casUnique: Buf): Future[CasResult] =
    checkAndSet(key, 0, Time.epoch, value, casUnique)

  /**
   * Get a key from the server.
   */
  def get(key: String): Future[Option[T]] = get(Seq(key)).map { _.values.headOption }

  /**
   * Get a key from the server, with a "cas unique" token.  The token
   * is treated opaquely by the memcache client but is in reality a
   * string-encoded u64.
   */
  def gets(key: String): Future[Option[(T, Buf)]] =
    gets(Seq(key)).map { _.values.headOption }

  /**
   * Get a set of keys from the server.
   * @return a Map[String, T] of all of the keys that the server had.
   */
  def get(keys: Iterable[String]): Future[Map[String, T]] = {
    getResult(keys) flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.values.mapValues { bufferToType(_) })
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
   */
  def gets(keys: Iterable[String]): Future[Map[String, (T, Buf)]] = {
    getsResult(keys) flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.valuesWithTokens.mapValues {
          case (v, u) => (bufferToType(v), u)
        })
      }
    }
  }

  /**
   * Get a set of keys from the server. Returns a Future[GetResult] that
   * encapsulates hits, misses and failures.
   */
  def getResult(keys: Iterable[String]): Future[GetResult]

  /**
   * Get a set of keys from the server. Returns a Future[GetsResult] that
   * encapsulates hits, misses and failures. This variant includes the casToken
   * from memcached.
   */
  def getsResult(keys: Iterable[String]): Future[GetsResult]

  /**
   * Remove a key.
   * @return true if deleted, false if not found
   */
  def delete(key: String): Future[JBoolean]

  /**
   * Increment a key. Interpret the value as an Long if it is parseable.
   * This operation has no effect if there is no value there already.
   */
  def incr(key: String, delta: Long): Future[Option[JLong]]
  def incr(key: String): Future[Option[JLong]] = incr(key, 1L)

  /**
   * Decrement a key. Interpret the value as an JLong if it is parseable.
   * This operation has no effect if there is no value there already.
   */
  def decr(key: String, delta: Long): Future[Option[JLong]]
  def decr(key: String): Future[Option[JLong]] = decr(key, 1L)

  /**
   * Store a key. Override an existing values.
   * @return true
   */
  def set(key: String, value: T): Future[Unit] =
    set(key, 0, Time.epoch, value)

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  def add(key: String, value: T): Future[JBoolean] =
    add(key, 0, Time.epoch, value)

  /**
   * Append a set of bytes to the end of an existing key. If the key doesn't
   * exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def append(key: String, value: T): Future[JBoolean] =
    append(key, 0, Time.epoch, value)

  /**
   * Prepend a set of bytes to the beginning of an existing key. If the key
   * doesn't exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def prepend(key: String, value: T): Future[JBoolean] =
    prepend(key, 0, Time.epoch, value)

  /**
   * Replace an item if it exists. If it doesn't exist, the operation has no
   * effect.
   * @return true if stored, false if not stored
   */
  def replace(key: String, value: T): Future[JBoolean] = replace(key, 0, Time.epoch, value)

  /**
   * Send a quit command to the server. Alternative to release, for
   * protocol compatability.
   * @return none
   */
  def quit(): Future[Unit] = Future(release())

  /**
   * Send a stats command with optional arguments to the server
   * @return a sequence of strings, each of which is a line of output
   */
  def stats(args: Option[String]): Future[Seq[String]]
  def stats(args: String): Future[Seq[String]] = stats(Some(args))
  def stats(): Future[Seq[String]] = stats(None)

  /**
   * release the underlying service(s)
   */
  def release(): Unit
}

trait Client extends BaseClient[Buf] {
  def bufferToType(v: Buf) = v

  def adapt[T](bijection: Bijection[Buf, T]): BaseClient[T] =
    new ClientAdaptor[T](this, bijection)

  /** Adaptor to use String as values */
  def withStrings: BaseClient[String] = adapt(
    new Bijection[Buf, String] {
      def apply(a: Buf): String  = a match { case Buf.Utf8(s) => s }
      override def invert(b: String): Buf = Buf.Utf8(b)
    }
  )

  /** Adaptor to use Array[Byte] as values */
  def withBytes: BaseClient[Array[Byte]] = adapt(
    new Bijection[Buf, Array[Byte]] {
      def apply(a: Buf): Array[Byte]  = a.toArray
      override def invert(b: Array[Byte]): Buf = Buf.ByteArray.Owned(b)
    }
  )
}

trait ProxyClient extends Client {
  protected def proxyClient: Client

  def getResult(keys: Iterable[String]) = proxyClient.getResult(keys)

  def getsResult(keys: Iterable[String]) = proxyClient.getsResult(keys)

  def set(key: String, flags: Int, expiry: Time, value: Buf) = proxyClient.set(key, flags, expiry, value)

  def add(key: String, flags: Int, expiry: Time, value: Buf) = proxyClient.add(key, flags, expiry, value)

  def replace(key: String, flags: Int, expiry: Time, value: Buf) = proxyClient.replace(key, flags, expiry, value)

  def append(key: String, flags: Int, expiry: Time, value: Buf) = proxyClient.append(key, flags, expiry, value)

  def prepend(key: String, flags: Int, expiry: Time, value: Buf) = proxyClient.prepend(key, flags, expiry, value)

  def incr(key: String, delta: Long) = proxyClient.incr(key, delta)

  def decr(key: String, delta: Long) = proxyClient.decr(key, delta)

  def checkAndSet(key: String, flags: Int, expiry: Time, value: Buf, casUnique: Buf) =
    proxyClient.checkAndSet(key, flags, expiry, value, casUnique)

  def delete(key: String) = proxyClient.delete(key)

  def stats(args: Option[String]) = proxyClient.stats(args)

  def release() { proxyClient.release() }
}

private[memcached] object ClientConstants {
  val JavaTrue: Future[JBoolean] = Future.value(true)
  val JavaFalse: Future[JBoolean] = Future.value(false)

  val FutureExists: Future[CasResult]   = Future.value(CasResult.Exists)
  val FutureNotFound: Future[CasResult] = Future.value(CasResult.NotFound)
  val FutureStored: Future[CasResult]   = Future.value(CasResult.Stored)

  val CasFromCheckAndSet: CasResult => Future[JBoolean] = {
    case CasResult.Stored   => JavaTrue
    case CasResult.Exists   => JavaFalse
    case CasResult.NotFound => JavaFalse
  }
}

/**
 * A Client connected to an individual Memcached server.
 *
 * @param  service  the underlying Memcached Service.
 */
protected class ConnectedClient(protected val service: Service[Command, Response]) extends Client {
  import ClientConstants._
  import scala.collection.breakOut

  protected def rawGet(command: RetrievalCommand) = {
    val keys: immutable.Set[String] = command.keys.map { case Buf.Utf8(s) => s }(breakOut)

    service(command).map {
      case Values(values) =>
        val hits: Map[String, Value] = values.map { case value =>
          val Buf.Utf8(keyStr) = value.key
          (keyStr, value)
        }(breakOut)
        val misses = util.NotFound(keys, hits.keySet)
        GetResult(hits, misses)
      case Error(e) => throw e
      case other    =>
        throw new IllegalStateException(
          "Invalid response type from get: %s".format(other.getClass.getSimpleName)
        )
    } handle {
      case t: RequestException => GetResult(failures = (keys.map { (_, t) }).toMap)
      case t: ChannelException => GetResult(failures = (keys.map { (_, t) }).toMap)
      case t: ServiceException => GetResult(failures = (keys.map { (_, t) }).toMap)
    }
  }

  def getResult(keys: Iterable[String]): Future[GetResult] = {
    try {
      if (keys==null) throw new IllegalArgumentException("Invalid keys: keys cannot be null")
      rawGet(Get(keys))
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For keys: " + keys))
    }
  }
  def getsResult(keys: Iterable[String]): Future[GetsResult] = {
    try {
      if (keys==null) throw new IllegalArgumentException("Invalid keys: keys cannot be null")
      rawGet(Gets(keys)).map { GetsResult(_) }
    }  catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For keys: " + keys))
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: Buf): Future[Unit] = {
    try {
      service(Set(key, flags, expiry, value)).map {
        case Stored() => ()
        case Error(e) => throw e
        case response => throw new IllegalStateException(s"Invalid response: $response")
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def checkAndSet(key: String, flags: Int, expiry: Time, value: Buf, casUnique: Buf): Future[CasResult] = {
    try {
      service(Cas(key, flags, expiry, value, casUnique)).flatMap {
        case Stored()   => FutureStored
        case Exists()   => FutureExists
        case NotFound() => FutureNotFound
        case Error(e)   => Future.exception(e)
        case _          => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def add(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] = {
    try {
      service(Add(key, flags, expiry, value)).flatMap {
        case Stored()     => JavaTrue
        case NotStored()  => JavaFalse
        case Error(e)     => Future.exception(e)
        case _            => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def append(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] = {
    try {
      service(Append(key, flags, expiry, value)).flatMap {
        case Stored()     => JavaTrue
        case NotStored()  => JavaFalse
        case Error(e)     => Future.exception(e)
        case _            => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def prepend(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] = {
    try {
      service(Prepend(key, flags, expiry, value)).flatMap {
        case Stored()     => JavaTrue
        case NotStored()  => JavaFalse
        case Error(e)     => Future.exception(e)
        case _            => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def replace(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] = {
    try {
      service(Replace(key, flags, expiry, value)).flatMap {
        case Stored()     => JavaTrue
        case NotStored()  => JavaFalse
        case Error(e)     => Future.exception(e)
        case _            => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def delete(key: String): Future[JBoolean] = {
    try {
      service(Delete(key)).flatMap {
        case Deleted()    => JavaTrue
        case NotFound()   => JavaFalse
        case Error(e)     => Future.exception(e)
        case _            => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def incr(key: String, delta: Long): Future[Option[JLong]] = {
    try {
      service(Incr(key, delta)).flatMap {
        case Number(value) => Future.value(Some(value))
        case NotFound()    => Future.None
        case Error(e)      => Future.exception(e)
        case _             => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def decr(key: String, delta: Long): Future[Option[JLong]] = {
    try {
      service(Decr(key, delta)).flatMap {
        case Number(value) => Future.value(Some(value))
        case NotFound()    => Future.None
        case Error(e)      => Future.exception(e)
        case _             => Future.exception(new IllegalStateException)
      }
    } catch {
      case t: IllegalArgumentException => Future.exception(new ClientError(t.getMessage + " For key: " + key))
    }
  }

  def stats(args: Option[String]): Future[Seq[String]] = {
    val statArgs: Seq[Buf] = args match {
      case None => Seq(Buf.Empty)
      case Some(args) => args.split(" ").map(nonEmptyStringToBuf).toSeq
    }
    service(Stats(statArgs)).flatMap {
      case InfoLines(lines) => Future {
        lines.map { line =>
          val key = line.key
          val values = line.values
          val Buf.Utf8(keyStr) = key
          "%s %s".format(keyStr, values.map { case Buf.Utf8(str) => str } mkString (" "))
        }
      }
      case Error(e) => Future.exception(e)
      case Values(list) => Future.Nil
      case _ => Future.exception(new IllegalStateException)
    }
  }

  def release(): Unit = {
    service.close()
  }
}

/**
 * A partitioned client is a client that delegates to an actual client based on
 * the key value.  Subclasses implement clientOf to choose the Client.
 */
trait PartitionedClient extends Client {
  protected[memcached] def clientOf(key: String): Client

  private[this] def withKeysGroupedByClient[A](
    keys: Iterable[String])(f: (Client, Iterable[String]) => Future[A]
  ): Future[Seq[A]] = {
    Future.collect(
      keys.groupBy(clientOf).map(Function.tupled(f)).toSeq
    )
  }

  def getResult(keys: Iterable[String]) = {
    if (keys.nonEmpty) {
      withKeysGroupedByClient(keys) {
        _.getResult(_)
      }.map { GetResult.merged(_) }
    } else {
      Future.value(GetResult.Empty)
    }
  }

  def getsResult(keys: Iterable[String]) = {
    if (keys.nonEmpty) {
      withKeysGroupedByClient(keys) {
         _.getsResult(_)
      }.map { GetResult.merged(_) }
    } else {
      Future.value(GetsResult(GetResult.Empty))
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: Buf) =
    clientOf(key).set(key, flags, expiry, value)
  def add(key: String, flags: Int, expiry: Time, value: Buf) =
    clientOf(key).add(key, flags, expiry, value)
  def append(key: String, flags: Int, expiry: Time, value: Buf) =
    clientOf(key).append(key, flags, expiry, value)
  def prepend(key: String, flags: Int, expiry: Time, value: Buf) =
    clientOf(key).prepend(key, flags, expiry, value)
  def replace(key: String, flags: Int, expiry: Time, value: Buf) =
    clientOf(key).replace(key, flags, expiry, value)
  def checkAndSet(key: String, flags: Int, expiry: Time, value: Buf, casUnique: Buf) =
    clientOf(key).checkAndSet(key, flags, expiry, value, casUnique)

  def delete(key: String)            = clientOf(key).delete(key)
  def incr(key: String, delta: Long) = clientOf(key).incr(key, delta)
  def decr(key: String, delta: Long) = clientOf(key).decr(key, delta)

  def stats(args: Option[String]): Future[Seq[String]] =
    throw new UnsupportedOperationException("No logical way to perform stats without a key")
}

object PartitionedClient {
  @deprecated("Use CacheNodeGroup.apply(hostPartWeights) instead", "7.0.0")
  def parseHostPortWeights(hostPortWeights: String): Seq[(String, Int, Int)] =
    hostPortWeights
      .split(Array(' ', ','))
      .filter((_ != ""))
      .map(_.split(":"))
      .map {
        case Array(host)               => (host, 11211, 1)
        case Array(host, port)         => (host, port.toInt, 1)
        case Array(host, port, weight) => (host, port.toInt, weight.toInt)
      }
}

abstract class KetamaClientKey {
  def identifier: String
}
object KetamaClientKey {
  private[memcached] case class HostPortBasedKey(host: String, port: Int, weight: Int) extends KetamaClientKey {
    val identifier = if (port == 11211) host else host + ":" + port
  }
  private[memcached] case class CustomKey(identifier: String) extends KetamaClientKey

  def apply(host: String, port: Int, weight: Int): KetamaClientKey =
    HostPortBasedKey(host, port, weight)

  def apply(id: String) = CustomKey(id)

  def fromCacheNode(node: CacheNode): KetamaClientKey = node.key match {
    case Some(id) => KetamaClientKey(id)
    case None => KetamaClientKey(node.host, node.port, node.weight)
  }
}

private[finagle] sealed trait NodeEvent
private[finagle] sealed trait NodeHealth extends NodeEvent
private[finagle] case class NodeMarkedDead(key: KetamaClientKey) extends NodeHealth
private[finagle] case class NodeRevived(key: KetamaClientKey) extends NodeHealth

class FailureAccrualException(message: String) extends RequestException(message, cause = null)

private[finagle] object KetamaFailureAccrualFactory {
  /**
   * Configures a stackable KetamaFailureAccrual factory with the given
   * `key` and `healthBroker`. The rest of the context is extracted from
   * Stack.Params.
   */
  def module[Req, Rep](
    key: KetamaClientKey,
    healthBroker: Broker[NodeHealth]
  ): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      import FailureAccrualFactory.Param
      val role = FailureAccrualFactory.role
      val description: String = "Memcached ketama failure accrual"
      override def parameters: Seq[Stack.Param[_]] = Seq(
        implicitly[Stack.Param[param.Stats]],
        implicitly[Stack.Param[FailureAccrualFactory.Param]],
        implicitly[Stack.Param[param.Timer]],
        implicitly[Stack.Param[param.Label]],
        implicitly[Stack.Param[param.Logger]],
        implicitly[Stack.Param[param.ResponseClassifier]],
        implicitly[Stack.Param[Transporter.EndpointAddr]]
      )

      def make(params: Stack.Params, next: ServiceFactory[Req, Rep]) =
        params[FailureAccrualFactory.Param] match {
            case Param.Configured(policy) =>
              val Memcached.param.EjectFailedHost(ejectFailedHost) =
                params[Memcached.param.EjectFailedHost]
              val timer = params[finagle.param.Timer].timer
              val stats = params[finagle.param.Stats].statsReceiver
              val classifier = params[finagle.param.ResponseClassifier].responseClassifier

              val label = params[finagle.param.Label].label
              val logger = params[finagle.param.Logger].log
              val endpoint = params[Transporter.EndpointAddr].addr

              new KetamaFailureAccrualFactory[Req, Rep](
                underlying = next,
                policy = policy(),
                responseClassifier = classifier,
                statsReceiver = stats,
                timer = timer,
                key = key,
                healthBroker = healthBroker,
                ejectFailedHost = ejectFailedHost,
                label = label) {
                override def didMarkDead(): Unit = {
                  logger.log(Level.INFO,
                    s"""FailureAccrualFactory marking connection to "$label" as dead. """+
                    s"""Remote Address: $endpoint. """+
                    s"""Eject failed host from ring: $ejectFailedHost""")
                  super.didMarkDead()
                }
              }

            case Param.Replaced(f) =>
              val timer = params[finagle.param.Timer].timer
              f(timer).andThen(next)

            case Param.Disabled => next
        }
    }
}

/**
 * A FailureAccrual module that can additionally communicate `NodeHealth` via
 * `healthBroker`. The broker is shared between the `KetamaPartitionedClient` and
 * allows for unhealthy nodes to be ejected from the ring if ejectFailedHost is true.
 */
private[finagle] class KetamaFailureAccrualFactory[Req, Rep](
    underlying: ServiceFactory[Req, Rep],
    policy: FailureAccrualPolicy,
    responseClassifier: ResponseClassifier,
    timer: Timer,
    statsReceiver: StatsReceiver,
    key: KetamaClientKey,
    healthBroker: Broker[NodeHealth],
    ejectFailedHost: Boolean,
    label: String)
  extends FailureAccrualFactory[Req, Rep](
    underlying,
    policy,
    responseClassifier,
    timer,
    statsReceiver)
{
  import FailureAccrualFactory._

  private[this] val failureAccrualEx =
    Future.exception(new FailureAccrualException("Endpoint is marked dead by failureAccrual") { serviceName = label })

  // exclude CancelledRequestException and CancelledConnectionException for cache client failure accrual
  override def isSuccess(reqRep: ReqRep): Boolean = reqRep.response match {
    case Return(_) => true
    case Throw(f: Failure) if f.cause.exists(_.isInstanceOf[CancelledRequestException]) && f.isFlagged(Failure.Interrupted) => true
    case Throw(f: Failure) if f.cause.exists(_.isInstanceOf[CancelledConnectionException]) && f.isFlagged(Failure.Interrupted) => true
      // Failure.InterruptedBy(_) would subsume all these eventually after rb/334371
    case Throw(WriteException(_: CancelledRequestException)) => true
    case Throw(_: CancelledRequestException) => true
    case Throw(WriteException(_: CancelledConnectionException)) => true
    case Throw(_: CancelledConnectionException) => true
    case Throw(e) => false
  }

  override protected def didMarkDead() {
    if (ejectFailedHost) healthBroker ! NodeMarkedDead(key)
  }

  // When host ejection is on, the host should be returned to the ring
  // immediately after it is woken, so it can satisfy a probe request
  override def startProbing() = synchronized {
    super.startProbing()
    if (ejectFailedHost) healthBroker ! NodeRevived(key)
  }

  override def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    getState match {
      case Alive | ProbeOpen  => super.apply(conn)
      // One finagle client presents one node on the Ketama ring,
      // the load balancer has one cache client. When the client
      // is in a busy state, continuing to dispatch requests is likely
      // to fail again. Thus we fail immediately if failureAccrualFactory
      // is in a busy state, which is triggered when failureCount exceeds
      // a threshold.
      case _ => failureAccrualEx
    }
}

private[finagle] object KetamaPartitionedClient {

  private object NodeState extends Enumeration {
    type t = this.Value
    val Live, Ejected = Value
  }

  private case class Node(node: KetamaNode[Client], var state: NodeState.Value)

  val DefaultNumReps = 160

  val shardNotAvailableDistributor: Distributor[Client] = {
    val failedService = new FailedService(new ShardNotAvailableException)
    new SingletonDistributor(TwemcacheClient(failedService): Client)
  }
}

/**
 * A partitioned client which implements consistent hashing across `cacheNodeGroup`.
 * The group is dynamic and the hash ring is rebuilt upon observed changes to
 * the group. It's also possible to communicate node health to this client by wiring
 * in a `nodeHealthBroker`. Unhealthy nodes are removed from the hash ring.
 *
 * TODO: This partitioning scheme should be moved inside the finagle stack
 * so that we can support non-bound names.
 */
private[finagle] class KetamaPartitionedClient(
    cacheNodeGroup: Group[CacheNode],
    newService: CacheNode => Service[Command, Response],
    nodeHealthBroker: Broker[NodeHealth] = new Broker[NodeHealth],
    statsReceiver: StatsReceiver = NullStatsReceiver,
    keyHasher: KeyHasher = KeyHasher.KETAMA,
    numReps: Int = KetamaPartitionedClient.DefaultNumReps,
    oldLibMemcachedVersionComplianceMode: Boolean = false)
  extends PartitionedClient { self =>

  import KetamaPartitionedClient._

  // exposed for testing
  private[memcached] val ketamaNodeGrp: Group[(KetamaClientKey, KetamaNode[Client])] =
    cacheNodeGroup.map { node =>
      val key = KetamaClientKey.fromCacheNode(node)
      val underlying = TwemcacheClient(newService(node))
      key -> KetamaNode(key.identifier, node.weight, underlying)
    }

  private[this] val nodes = mutable.Map[KetamaClientKey, Node]()

  // We update those out of the request path so we need to make sure to synchronize on
  // read-modify-write operations on `currentDistributor` and `distributor`.
  // Note: Volatile-read from `clientOf` safety (not raciness) is guaranteed by JMM.
  @volatile private[this] var currentDistributor: Distributor[Client] =
    shardNotAvailableDistributor
  @volatile private[this] var snapshot: immutable.Set[(KetamaClientKey, KetamaNode[Client])] =
    immutable.Set.empty

  private[this] val ejectionCount = statsReceiver.counter("ejections")
  private[this] val revivalCount = statsReceiver.counter("revivals")
  private[this] val nodeLeaveCount = statsReceiver.counter("leaves")
  private[this] val nodeJoinCount = statsReceiver.counter("joins")
  private[this] val keyRingRedistributeCount = statsReceiver.counter("redistributes")

  statsReceiver.addGauge("live_nodes") {
    self.synchronized { nodes.count { case (_, Node(_, state)) => state == NodeState.Live } }
  }

  statsReceiver.addGauge("dead_nodes") {
    self.synchronized { nodes.count { case (_, Node(_, state)) => state == NodeState.Ejected } }
  }

  private[this] val ketamaNodeGrpChanges: Event[immutable.Set[((KetamaClientKey, KetamaNode[Client]))]] =
    ketamaNodeGrp.set.changes.filter(_.nonEmpty)

  // We listen for changes on a group to update the cache ring.
  private[this] val listener: Closable = ketamaNodeGrpChanges.respond(updateGroup)

  // We also listen on a broker to eject/revive cache nodes.
  nodeHealthBroker.recv.foreach {
    case NodeMarkedDead(key) => ejectNode(key)
    case NodeRevived(key) => reviveNode(key)
  }

  override def clientOf(key: String): Client = {
    val bytes = key.getBytes(Charsets.Utf8)
    val hash = keyHasher.hashKey(bytes)
    currentDistributor.nodeForHash(hash)
  }

  private[this] def rebuildDistributor(): Unit = self.synchronized {
    keyRingRedistributeCount.incr()

    val liveNodes = nodes.collect({ case (_, Node(node, NodeState.Live)) => node })(breakOut)

    currentDistributor =
      if (liveNodes.isEmpty) shardNotAvailableDistributor
      else new KetamaDistributor(liveNodes, numReps, oldLibMemcachedVersionComplianceMode)
  }

  private[this] def updateGroup(current: immutable.Set[(KetamaClientKey, KetamaNode[Client])]): Unit =
    self.synchronized {
      val old = snapshot
      // remove old nodes and release clients
      nodes --= (old &~ current).collect {
        case (key, node) =>
          node.handle.release()
          nodeLeaveCount.incr()
          key
      }

      // new joined node appears as Live state
      nodes ++= (current &~ old).collect {
        case (key, node) =>
          nodeJoinCount.incr()
          key -> Node(node, NodeState.Live)
      }

      snapshot = current
      rebuildDistributor()
    }

  private[this] def ejectNode(key: KetamaClientKey) = self.synchronized {
    nodes.get(key) match {
      case Some(node) if node.state == NodeState.Live =>
        node.state = NodeState.Ejected
        rebuildDistributor()
        ejectionCount.incr()
      case _ =>
    }
  }

  private[this] def reviveNode(key: KetamaClientKey) = self.synchronized {
    nodes.get(key) match {
      case Some(node) if node.state == NodeState.Ejected =>
        node.state = NodeState.Live
        rebuildDistributor()
        revivalCount.incr()
      case _ =>
    }
  }

  // await for the readiness of initial loading of ketamaNodeGrp prior to first request
  // this readiness here will be fulfilled the first time the ketamaNodeGrp is updated
  // with non-empty content, after that group can still be updated with empty endpoints
  // which will throw NoShardAvailableException to users indicating the lost of cache access
  val ready = ketamaNodeGrpChanges.toFuture().unit

  override def getsResult(keys: Iterable[String]) =
    ready.interruptible().before(super.getsResult(keys))

  override def getResult(keys: Iterable[String]) =
    ready.interruptible().before(super.getResult(keys))

  override def set(key: String, flags: Int, expiry: Time, value: Buf) =
    ready.interruptible().before(super.set(key, flags, expiry, value))

  override def delete(key: String) =
    ready.interruptible().before(super.delete(key))

  override def checkAndSet(key: String, flags: Int, expiry: Time, value: Buf, casUnique: Buf) =
    ready.interruptible().before(super.checkAndSet(key, flags, expiry, value, casUnique))

  override def add(key: String, flags: Int, expiry: Time, value: Buf) =
    ready.interruptible().before(super.add(key, flags, expiry, value))

  override def replace(key: String, flags: Int, expiry: Time, value: Buf) =
    ready.interruptible().before(super.replace(key, flags, expiry, value))

  override def prepend(key: String, flags: Int, expiry: Time, value: Buf) =
    ready.interruptible().before(super.prepend(key, flags, expiry, value))

  override def append(key: String, flags: Int, expiry: Time, value: Buf) =
    ready.interruptible().before(super.append(key, flags, expiry, value))

  override def incr(key: String, delta: Long) =
    ready.interruptible().before(super.incr(key, delta))

  override def decr(key: String, delta: Long) =
    ready.interruptible().before(super.decr(key, delta))

  def release(): Unit = synchronized {
    nodes.foreach { case (_, n) =>
      n.node.handle.release()
    }

    listener.close()
  }
}

object KetamaClient {
  val DefaultNumReps = KetamaPartitionedClient.DefaultNumReps
}

@deprecated(message = "Use the `com.twitter.finagle.Memcached builder", since = "2015-02-22")
case class KetamaClientBuilder private[memcached](
  _group: Group[CacheNode],
  _hashName: Option[String],
  _clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]],
  _failureAccrualParams: (Int, () => Duration) = (5, () => 30.seconds),
  _ejectFailedHost: Boolean = true,
  oldLibMemcachedVersionComplianceMode: Boolean = false,
  numReps: Int = KetamaPartitionedClient.DefaultNumReps
) {
  import Memcached.Client.mkDestination

  private lazy val localMemcachedName = Resolver.eval("localhost:" + LocalMemcached.port)

  private def withLocalMemcached = {
    val Name.Bound(va) = localMemcachedName
    copy(_group = CacheNodeGroup.fromVarAddr(va))
  }

  def dest(
    name: Name,
    useOnlyResolvedAddress: Boolean = false
  ): KetamaClientBuilder = {
    val Name.Bound(va) = if (LocalMemcached.enabled) {
      localMemcachedName
    } else {
      name
    }
    copy(_group = CacheNodeGroup.fromVarAddr(va, useOnlyResolvedAddress))
  }

  def dest(name: String): KetamaClientBuilder =
    if (LocalMemcached.enabled) {
      withLocalMemcached
    } else dest(Resolver.eval(name))

  @deprecated("Use `KetamaClientBuilder.dest(name: Name)` instead", "7.0.0")
  def group(group: Group[CacheNode]): KetamaClientBuilder = {
    if (LocalMemcached.enabled) {
      withLocalMemcached
    } else {
      copy(_group = group)
    }
  }

  @deprecated("Use `KetamaClientBuilder.dest(name: Name)` instead", "7.0.0")
  def cluster(cluster: Cluster[InetSocketAddress]): KetamaClientBuilder = {
    group(CacheNodeGroup(Group.fromCluster(cluster).map{_.asInstanceOf[SocketAddress]}))
  }

  @deprecated("Use `KetamaClientBuilder.dest(name: Name)` instead", "7.0.0")
  def cachePoolCluster(cluster: Cluster[CacheNode]): KetamaClientBuilder = {
    if (LocalMemcached.enabled) {
      withLocalMemcached
    } else {
      copy(_group = Group.fromCluster(cluster))
    }
  }

  def nodes(nodes: Seq[(String, Int, Int)]): KetamaClientBuilder = {
    if (LocalMemcached.enabled) {
      withLocalMemcached
    } else {
      copy(_group = Group(nodes.map {
        case (host, port, weight) => new CacheNode(host, port, weight)
      }: _*))
    }
  }

  def nodes(hostPortWeights: String): KetamaClientBuilder =
    group(CacheNodeGroup(hostPortWeights))

  def hashName(hashName: String): KetamaClientBuilder =
    copy(_hashName = Some(hashName))

  def numReps(numReps: Int): KetamaClientBuilder =
    copy(numReps = numReps)

  def clientBuilder(clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]): KetamaClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def failureAccrualParams(numFailures: Int, markDeadFor: Duration): KetamaClientBuilder =
    copy(_failureAccrualParams = (numFailures, () => markDeadFor))

  def failureAccrualParams(numFailures: Int, markDeadFor: () => Duration): KetamaClientBuilder =
    copy(_failureAccrualParams = (numFailures, markDeadFor))

  def noFailureAccrual: KetamaClientBuilder =
    copy(_failureAccrualParams = (Int.MaxValue, () => Duration.Zero))

  def enableOldLibMemcachedVersionComplianceMode(): KetamaClientBuilder =
    copy(oldLibMemcachedVersionComplianceMode = true)

  def ejectFailedHost(eject: Boolean): KetamaClientBuilder =
    copy(_ejectFailedHost = eject)

  def build(): Client = {
    val stackBasedClient =
      (_clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1).daemon(true))
        .codec(text.Memcached())
        .underlying

    val keyHasher = KeyHasher.byName(_hashName.getOrElse("ketama"))

    val (numFailures, markDeadFor) = _failureAccrualParams

    val label = stackBasedClient.params[finagle.param.Label].label
    val stats = stackBasedClient.params[finagle.param.Stats]
      .statsReceiver.scope(label).scope("memcached_client")

    val healthBroker = new Broker[NodeHealth]

    Memcached.registerClient(label, keyHasher.toString, isPipelining = false)

    def newService(node: CacheNode) = stackBasedClient
      .configured(Memcached.param.EjectFailedHost(_ejectFailedHost))
      .configured(FailureAccrualFactory.Param(numFailures, markDeadFor))
      .configured(FactoryToService.Enabled(true))
      .transformed(new Stack.Transformer {
        val key = KetamaClientKey.fromCacheNode(node)
        def apply[Cmd, Rep](stk: Stack[ServiceFactory[Cmd, Rep]]) =
          stk.replace(FailureAccrualFactory.role,
            KetamaFailureAccrualFactory.module[Cmd, Rep](key, healthBroker))
      }).newClient(mkDestination(node.host, node.port)).toService

    new KetamaPartitionedClient(
      _group,
      newService,
      healthBroker,
      stats,
      keyHasher,
      numReps,
      oldLibMemcachedVersionComplianceMode)
  }
}

object KetamaClientBuilder {
  def apply(): KetamaClientBuilder = KetamaClientBuilder(Group.empty, Some("ketama"), None)
  def get() = apply()
}

/**
 * Ruby memcache-client (MemCache) compatible client.
 */
class RubyMemCacheClient(clients: Seq[Client]) extends PartitionedClient {
  protected[memcached] def clientOf(key: String) = {
    val bytes = key.getBytes(Charsets.Utf8)
    val hash = (KeyHasher.CRC32_ITU.hashKey(bytes) >> 16) & 0x7fff
    val index = hash % clients.size
    clients(index.toInt)
  }

  def release() {
    clients foreach { _.release() }
  }
}

/**
 * Builder for memcache-client (MemCache) compatible client.
 */
case class RubyMemCacheClientBuilder(
  _nodes: Seq[(String, Int, Int)],
  _clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]]) {

  def this() = this(
    Nil,  // nodes
    None  // clientBuilder
  )

  def nodes(nodes: Seq[(String, Int, Int)]): RubyMemCacheClientBuilder =
    copy(_nodes = nodes)

  def nodes(hostPortWeights: String): RubyMemCacheClientBuilder =
    copy(_nodes = CacheNodeGroup(hostPortWeights).members.map {
      node: CacheNode => (node.host, node.port, node.weight)
    }.toSeq)

  def clientBuilder(clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]): RubyMemCacheClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def build(): PartitionedClient = {
    val builder = _clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1).daemon(true)
    val clients = _nodes.map { case (hostname, port, weight) =>
      require(weight == 1, "Ruby memcache node weight must be 1")
      Client(builder.hosts(hostname + ":" + port).codec(text.Memcached()).build())
    }
    new RubyMemCacheClient(clients)
  }
}

/**
 * PHP memcache-client (memcache.so) compatible client.
 */
class PHPMemCacheClient(clients: Array[Client], keyHasher: KeyHasher)
  extends PartitionedClient {
  protected[memcached] def clientOf(key: String) = {
    // See mmc_hash() in memcache_standard_hash.c
    val hash = (keyHasher.hashKey(key.getBytes) >> 16) & 0x7fff
    val index = hash % clients.size
    clients(index.toInt)
  }

  def release() {
    clients foreach { _.release() }
  }
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
    copy(_nodes = CacheNodeGroup(hostPortWeights).members.map {
      node: CacheNode => (node.host, node.port, node.weight)
    }.toSeq)

  def hashName(hashName: String): PHPMemCacheClientBuilder =
    copy(_hashName = Some(hashName))

  def clientBuilder(clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]): PHPMemCacheClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def build(): PartitionedClient = {
    val builder = _clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1).daemon(true)
    val keyHasher = KeyHasher.byName(_hashName.getOrElse("crc32-itu"))
    val clients = _nodes.map { case (hostname, port, weight) =>
      val client = Client(builder.hosts(hostname + ":" + port).codec(text.Memcached()).build())
      for (i <- (1 to weight)) yield client
    }.flatten.toArray
    new PHPMemCacheClient(clients, keyHasher)
  }
}

object PHPMemCacheClientBuilder {
  def apply(): PHPMemCacheClientBuilder = PHPMemCacheClientBuilder(Nil, Some("crc32-itu"), None)
  def get() = apply()
}
