package com.twitter.finagle.memcached

import scala.collection.JavaConversions._
import scala.collection.immutable

import _root_.java.util.{Map => JMap}

import com.twitter.finagle.{ChannelException, RequestException, ServiceException}
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.Service
import com.twitter.hashing._
import com.twitter.util.{Time, Future, Bijection}

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil.UTF_8

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
      .codec(new Memcached)
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
  lazy val values = hits mapValues { _.value }

  def ++(o: GetResult) = GetResult(hits ++ o.hits, misses ++ o.misses, failures ++ o.failures)
}

case class GetsResult(getResult: GetResult) {
  def hits = getResult.hits
  def misses = getResult.misses
  def failures = getResult.failures
  def values = getResult.values
  lazy val valuesWithTokens = hits mapValues { v => (v.value, v.casUnique.get) }
  def ++(o: GetsResult) = GetsResult(getResult ++ o.getResult)
}

object GetResult {
  private[memcached] def merged(results: Seq[GetResult]): GetResult = {
    results.foldLeft(GetResult()) { _ ++ _ }
  }

  private[memcached] def merged(results: Seq[GetsResult]) = {
    val unwrapped = results map { _.getResult }
    GetsResult(
      unwrapped.foldLeft(GetResult()) { _ ++ _ }
    )
  }
}

/**
 * A friendly client to talk to a Memcached server.
 */
trait BaseClient[T] {
  def channelBufferToType(a: ChannelBuffer): T

  /**
   * Store a key. Override an existing value.
   * @return true
   */
  def set(key: String, flags: Int, expiry: Time, value: T): Future[Unit]

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  def add(key: String, flags: Int, expiry: Time, value: T): Future[Boolean]

  /**
   * Append bytes to the end of an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  def append(key: String, flags: Int, expiry: Time, value: T): Future[Boolean]

  /**
   * Prepend bytes to the beginning of an existing key. If the key doesn't
   * exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def prepend(key: String, flags: Int, expiry: Time, value: T): Future[Boolean]

  /**
   * Replace bytes on an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  def replace(key: String, flags: Int, expiry: Time, value: T): Future[Boolean]

  /**
   * Perform a CAS operation on the key, only if the value has not
   * changed since the value was last retrieved, and `casUnique`
   * extracted from a `gets` command.  We treat the "cas unique" token
   * opaquely, but in reality it is a string-encoded u64.
   *
   * @return true if replaced, false if not
   */
  def cas(
    key: String, flags: Int, expiry: Time, value: T, casUnique: ChannelBuffer
  ): Future[Boolean]

  /**
   * Get a key from the server.
   */
  def get(key: String): Future[Option[T]] = get(Seq(key))  map { _.values.headOption }

  /**
   * Get a key from the server, with a "cas unique" token.  The token
   * is treated opaquely by the memcache client but is in reality a
   * string-encoded u64.
   */
  def gets(key: String): Future[Option[(T, ChannelBuffer)]] =
    gets(Seq(key)) map { _.values.headOption }

  /**
   * Get a set of keys from the server.
   * @return a Map[String, T] of all of the keys that the server had.
   */
  def get(keys: Iterable[String]): Future[Map[String, T]] = {
    getResult(keys) flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.values mapValues { channelBufferToType(_) })
      }
    }
  }

  /**
   * Get a set of keys from the server, together with a "cas unique"
   * token.  The token is treated opaquely by the memcache client but
   * is in reality a string-encoded u64.
   *
   * @return a Map[String, (T, ChannelBuffer)] of all the
   * keys the server had, together with their "cas unique" token
   */
  def gets(keys: Iterable[String]): Future[Map[String, (T, ChannelBuffer)]] = {
    getsResult(keys) flatMap { result =>
      if (result.failures.nonEmpty) {
        Future.exception(result.failures.values.head)
      } else {
        Future.value(result.valuesWithTokens mapValues {
          case (v, u) => (channelBufferToType(v), u)
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
  def delete(key: String): Future[Boolean]

  /**
   * Increment a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  def incr(key: String, delta: Long): Future[Option[Long]]
  def incr(key: String): Future[Option[Long]] = incr(key, 1L)

  /**
   * Decrement a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  def decr(key: String, delta: Long): Future[Option[Long]]
  def decr(key: String): Future[Option[Long]] = decr(key, 1L)

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
  def add(key: String, value: T): Future[Boolean] =
    add(key, 0, Time.epoch, value)

  /**
   * Append a set of bytes to the end of an existing key. If the key doesn't
   * exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def append(key: String, value: T): Future[Boolean] =
    append(key, 0, Time.epoch, value)

  /**
   * Prepend a set of bytes to the beginning of an existing key. If the key
   * doesn't exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def prepend(key: String, value: T): Future[Boolean] =
    prepend(key, 0, Time.epoch, value)

  /**
   * Replace an item if it exists. If it doesn't exist, the operation has no
   * effect.
   * @return true if stored, false if not stored
   */
  def replace(key: String, value: T): Future[Boolean] = replace(key, 0, Time.epoch, value)

  /**
   * Perform a CAS operation on the key, only if the value has not
   * changed since the value was last retrieved, and `casUnique`
   * extracted from a `gets` command.  We treat the "cas unique" token
   * opaquely, but in reality it is a string-encoded u64.
   *
   * @return true if replaced, false if not
   */
  def cas(key: String, value: T, casUnique: ChannelBuffer): Future[Boolean] =
    cas(key, 0, Time.epoch, value, casUnique)

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

trait Client extends BaseClient[ChannelBuffer] {
  def channelBufferToType(v: ChannelBuffer) = v

  def adapt[T](bijection: Bijection[ChannelBuffer, T]): BaseClient[T] =
    new ClientAdaptor[T](this, bijection)

  /** Adaptor to use String as values */
  def withStrings: BaseClient[String] = adapt(
    new Bijection[ChannelBuffer, String] {
      def apply(a: ChannelBuffer): String  = channelBufferToString(a)
      def invert(b: String): ChannelBuffer = stringToChannelBuffer(b)
    }
  )

  /** Adaptor to use Array[Byte] as values */
  def withBytes: BaseClient[Array[Byte]] = adapt(
    new Bijection[ChannelBuffer, Array[Byte]] {
      def apply(a: ChannelBuffer): Array[Byte]  = channelBufferToBytes(a)
      def invert(b: Array[Byte]): ChannelBuffer = bytesToChannelBuffer(b)
    }
  )
}

/**
 * A Client connected to an individual Memcached server.
 *
 * @param  underlying  the underlying Memcached Service.
 */
protected class ConnectedClient(service: Service[Command, Response]) extends Client {
  private[this] def rawGet(command: RetrievalCommand) = {
    val keys = immutable.Set(command.keys map { _.toString(UTF_8) }: _*)

    service(command) map {
      case Values(values) =>
        val tuples = values.map {
          case value => (value.key.toString(UTF_8), value)
        }
        val hits = tuples.toMap
        val misses = keys -- hits.keySet
        GetResult(hits, misses)
      case Error(e) => throw e
      case _        => throw new IllegalStateException
    } handle {
      case t: RequestException => GetResult(failures = (keys map { (_, t) }).toMap)
      case t: ChannelException => GetResult(failures = (keys map { (_, t) }).toMap)
      case t: ServiceException => GetResult(failures = (keys map { (_, t) }).toMap)
    }
  }

  def getResult(keys: Iterable[String]) = rawGet(Get(keys.toSeq))
  def getsResult(keys: Iterable[String]) =
    rawGet(Gets(keys.toSeq)) map { GetsResult(_) }

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    service(Set(key, flags, expiry, value)) map {
      case Stored() => ()
      case Error(e) => throw e
      case _        => throw new IllegalStateException
    }

  def cas(key: String, flags: Int, expiry: Time, value: ChannelBuffer, casUnique: ChannelBuffer) =
    service(Cas(key, flags, expiry, value, casUnique)) map {
      case Stored() => true
      case Exists() => false
      case Error(e) => throw e
      case _        => throw new IllegalStateException
    }

  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    service(Add(key, flags, expiry, value)) map {
      case Stored()     => true
      case NotStored()  => false
      case Error(e)     => throw e
      case _            => throw new IllegalStateException
    }

  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    service(Append(key, flags, expiry, value)) map {
      case Stored()     => true
      case NotStored()  => false
      case Error(e)     => throw e
      case _            => throw new IllegalStateException
    }

  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    service(Prepend(key, flags, expiry, value)) map {
      case Stored()     => true
      case NotStored()  => false
      case Error(e)     => throw e
      case _            => throw new IllegalStateException
    }

  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    service(Replace(key, flags, expiry, value)) map {
      case Stored()     => true
      case NotStored()  => false
      case Error(e)     => throw e
      case _            => throw new IllegalStateException
    }

  def delete(key: String) =
    service(Delete(key)) map {
      case Deleted()    => true
      case NotFound()   => false
      case Error(e)     => throw e
      case _            => throw new IllegalStateException
    }

  def incr(key: String, delta: Long): Future[Option[Long]] = {
    service(Incr(key, delta)) map {
      case Number(value) => Some(value)
      case NotFound()    => None
      case Error(e)      => throw e
      case _             => throw new IllegalStateException
    }
  }

  def decr(key: String, delta: Long): Future[Option[Long]] = {
    service(Decr(key, delta)) map {
      case Number(value) => Some(value)
      case NotFound()    => None
      case Error(e)      => throw e
      case _             => throw new IllegalStateException
    }
  }

  def stats(args: Option[String]): Future[Seq[String]] = {
    val statArgs: Seq[ChannelBuffer] = args match {
      case None => Seq(ChannelBuffers.EMPTY_BUFFER)
      case Some(args) => args.split(" ").toSeq
    }
    service(Stats(statArgs)) map {
      case InfoLines(lines) => lines.map { line =>
        val key = line.key
        val values = line.values
        key.toString(UTF_8) + " " + values.map { value => value.toString(UTF_8) }.mkString(" ")
      }
      case Error(e) => throw e
      case Values(list) => Nil
      case _ => throw new IllegalStateException
    }
  }

  def release() {
    service.release()
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
      keys groupBy(clientOf(_)) map Function.tupled(f) toSeq
    )
  }

  def getResult(keys: Iterable[String]) = {
    if (keys.nonEmpty) {
      withKeysGroupedByClient(keys) {
        _.getResult(_)
      } map { GetResult.merged(_) }
    } else {
      Future.value(GetResult())
    }
  }

  def getsResult(keys: Iterable[String]) = {
    if (keys.nonEmpty) {
      withKeysGroupedByClient(keys) {
         _.getsResult(_)
      } map { GetResult.merged(_) }
    } else {
      Future.value(GetsResult(GetResult()))
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    clientOf(key).set(key, flags, expiry, value)
  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    clientOf(key).add(key, flags, expiry, value)
  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    clientOf(key).append(key, flags, expiry, value)
  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    clientOf(key).prepend(key, flags, expiry, value)
  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    clientOf(key).replace(key, flags, expiry, value)
  def cas(key: String, flags: Int, expiry: Time, value: ChannelBuffer, casUnique: ChannelBuffer) =
    clientOf(key).cas(key, flags, expiry, value, casUnique)


  def delete(key: String)            = clientOf(key).delete(key)
  def incr(key: String, delta: Long) = clientOf(key).incr(key, delta)
  def decr(key: String, delta: Long) = clientOf(key).decr(key, delta)

  def stats(args: Option[String]): Future[Seq[String]] =
    throw new UnsupportedOperationException("No logical way to perform stats without a key")

}

object PartitionedClient {
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

case class KetamaClientKey(host: String, port: Int, weight: Int) {
  def toTuple = (host, port, weight)
}

class KetamaClient(clients: Map[(String, Int, Int), Client], keyHasher: KeyHasher = KeyHasher.KETAMA)
  extends PartitionedClient
{
  def this(clients: JMap[KetamaClientKey, Client]) =
      this((Map() ++ clients) map { case (k, v) => (k.toTuple, v) })

  require(!clients.isEmpty, "At least one client must be provided")

  private val NUM_REPS = 160

  protected val distributor = {
    val nodes = clients.map { case ((ip, port, weight), client) =>
      val identifier = if (port == 11211) ip else ip + ":" + port
      KetamaNode(identifier, weight, client)
    }.toList
    new KetamaDistributor(nodes, NUM_REPS)
  }

  protected[memcached] def clientOf(key: String) = {
    distributor.nodeForHash(keyHasher.hashKey(key))
  }

  def release() {
    clients.values foreach { _.release() }
  }
}

case class KetamaClientBuilder(
  _nodes: Seq[(String, Int, Int)],
  _hashName: Option[String],
  _clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]]) {

  @deprecated("Use `KetamaClientBuilder()` instead")
  def this() = this(
    Nil,            // nodes
    Some("ketama"), // hashName
    None            // clientBuilder
  )


  def nodes(nodes: Seq[(String, Int, Int)]): KetamaClientBuilder =
    copy(_nodes = nodes)

  def nodes(hostPortWeights: String): KetamaClientBuilder =
    copy(_nodes = PartitionedClient.parseHostPortWeights(hostPortWeights))

  def hashName(hashName: String): KetamaClientBuilder =
    copy(_hashName = Some(hashName))

  def clientBuilder(clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]): KetamaClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def build(): PartitionedClient = {
    val builder = _clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1)

    val clients = Map() ++ _nodes.map { case (hostname, port, weight) =>
      val b = builder.hosts(hostname + ":" + port).codec(new Memcached)
      val client = Client(b.build())
      ((hostname, port, weight) -> client)
    }
    val keyHasher = KeyHasher.byName(_hashName.getOrElse("ketama"))
    new KetamaClient(clients, keyHasher)
  }
}

object KetamaClientBuilder {
  def apply(): KetamaClientBuilder = KetamaClientBuilder(Nil, Some("ketama"), None)
  def get() = apply()
}


/**
 * Ruby memcache-client (MemCache) compatible client.
 */
class RubyMemCacheClient(clients: Seq[Client]) extends PartitionedClient {
  protected[memcached] def clientOf(key: String) = {
    val hash = (KeyHasher.CRC32_ITU.hashKey(key) >> 16) & 0x7fff
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
    copy(_nodes = PartitionedClient.parseHostPortWeights(hostPortWeights))

  def clientBuilder(clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]): RubyMemCacheClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def build(): PartitionedClient = {
    val builder = _clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1)
    val clients = _nodes.map { case (hostname, port, weight) =>
      require(weight == 1, "Ruby memcache node weight must be 1")
      Client(builder.hosts(hostname + ":" + port).codec(new Memcached).build())
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
    copy(_nodes = PartitionedClient.parseHostPortWeights(hostPortWeights))

  def hashName(hashName: String): PHPMemCacheClientBuilder =
    copy(_hashName = Some(hashName))

  def clientBuilder(clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]): PHPMemCacheClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def build(): PartitionedClient = {
    val builder = _clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1)
    val keyHasher = KeyHasher.byName(_hashName.getOrElse("crc32-itu"))
    val clients = _nodes.map { case (hostname, port, weight) =>
      val client = Client(builder.hosts(hostname + ":" + port).codec(new Memcached).build())
      for (i <- (1 to weight)) yield client
    }.flatten.toArray
    new PHPMemCacheClient(clients, keyHasher)
  }
}

object PHPMemCacheClientBuilder {
  def apply(): PHPMemCacheClientBuilder = PHPMemCacheClientBuilder(Nil, Some("crc32-itu"), None)
  def get() = apply()
}
