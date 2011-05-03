package com.twitter.finagle.memcached

import _root_.java.util.TreeMap
import _root_.java.security.MessageDigest
import _root_.java.nio.{ByteBuffer, ByteOrder}

import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConversions._
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import text.Memcached
import com.twitter.finagle.Service
import com.twitter.util.{Time, Future}

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

/**
 * A friendly client to talk to a Memcached server.
 */
trait Client {
  /**
   * Store a key. Override an existing value.
   * @return true
   */
  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer):     Future[Unit]

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer):     Future[Boolean]

  /**
   * Append bytes to the end of an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer):  Future[Boolean]

  /**
   * Prepend bytes to the beginning of an existing key. If the key doesn't
   * exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[Boolean]

  /**
   * Replace bytes on an existing key. If the key doesn't exist, the
   * operation has no effect.
   * @return true if stored, false if not stored
   */
  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[Boolean]

  /**
   * Get a key from the server.
   */
  def get(key: String):                           Future[Option[ChannelBuffer]]

  /**
   * Get a set of keys from the server.
   * @return a Map[String, ChannelBuffer] of all of the keys that the server had.
   */
  def get(keys: Iterable[String]):                Future[Map[String, ChannelBuffer]]

  /**
   * Remove a key.
   * @return true if deleted, false if not found
   */
  def delete(key: String):                        Future[Boolean]

  /**
   * Increment a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  def incr(key: String):                         Future[Option[Long]]
  def incr(key: String, delta: Long):            Future[Option[Long]]

  /**
   * Decrement a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  def decr(key: String):                         Future[Option[Long]]
  def decr(key: String, delta: Long):            Future[Option[Long]]

  /**
   * Store a key. Override an existing values.
   * @return true
   */
  def set(key: String, value: ChannelBuffer): Future[Unit] =
    set(key, 0, Time.epoch, value)

  /**
   * Store a key but only if it doesn't already exist on the server.
   * @return true if stored, false if not stored
   */
  def add(key: String, value: ChannelBuffer): Future[Boolean] =
    add(key, 0, Time.epoch, value)

  /**
   * Append a set of bytes to the end of an existing key. If the key doesn't
   * exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def append(key: String, value: ChannelBuffer): Future[Boolean] =
    append(key, 0, Time.epoch, value)

  /**
   * Prepend a set of bytes to the beginning of an existing key. If the key
   * doesn't exist, the operation has no effect.
   * @return true if stored, false if not stored
   */
  def prepend(key: String, value: ChannelBuffer): Future[Boolean] =
    prepend(key, 0, Time.epoch, value)

  /**
   * Replace an item if it exists. If it doesn't exist, the operation has no
   * effect.
   * @return true if stored, false if not stored
   */
  def replace(key: String, value: ChannelBuffer): Future[Boolean] = replace(key, 0, Time.epoch, value)
}

/**
 * A Client connected to an individual Memcached server.
 *
 * @param  underlying  the underlying Memcached Service.
 */
protected class ConnectedClient(service: Service[Command, Response]) extends Client {
  def get(key: String) = {
    service(Get(Seq(key))) map {
      case Values(values) =>
        if (values.size > 0) Some(values.head.value)
        else None
      case Error(e) => throw e
      case _        => throw new IllegalStateException
    }
  }

  def get(keys: Iterable[String]) = {
    service(Get(keys.toSeq)) map {
      case Values(values) =>
        val tuples = values.map {
          case Value(key, value) =>
            (key.toString(CharsetUtil.UTF_8), value)
        }
        Map(tuples: _*)
      case Error(e) => throw e
      case _        => throw new IllegalStateException
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    service(Set(key, flags, expiry, value)) map {
      case Stored() => ()
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

  def incr(key: String) = incr(key, 1L)

  def decr(key: String) = decr(key, 1L)

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
}

/**
 * A partitioned client is a client that delegates to an actual client based on
 * the key value.  Subclasses implement clientOf to choose the Client.
 */
trait PartitionedClient extends Client {
  protected[memcached] def clientOf(key: String): Client

  def get(key: String)                    = clientOf(key).get(key)
  def get(keys: Iterable[String])         = {
    if (!keys.isEmpty) {
      val keysGroupedByClient = keys.groupBy(clientOf(_))

      val mapOfMaps = keysGroupedByClient.map { case (client, keys) =>
        client.get(keys)
      }

      mapOfMaps.reduceLeft { (result, nextMap) =>
        for {
          result <- result
          nextMap <- nextMap
        } yield {
          result ++ nextMap
        }
      }
    } else {
      Future(Map[String, ChannelBuffer]())
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

  def delete(key: String)             = clientOf(key).delete(key)
  def incr(key: String)              = clientOf(key).incr(key)
  def incr(key: String, delta: Long) = clientOf(key).incr(key, delta)
  def decr(key: String)              = clientOf(key).decr(key)
  def decr(key: String, delta: Long) = clientOf(key).decr(key, delta)
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


class KetamaClient(clients: Map[(String, Int, Int), Client], keyHasher: KeyHasher = KeyHasher.KETAMA)
  extends PartitionedClient
{
  require(!clients.isEmpty, "At least one client must be provided")

  // we use (NUM_REPS * #servers) total points, but allocate them based on server weights.
  val NUM_REPS = 160

  protected val continuum = {
    var continuum = new TreeMap[Long, (String, Int, Int)]()
    val serverCount = clients.size
    val totalWeight = clients.keys.foldLeft(0.0) {_+_._3}

    for ((hostname, port, weight) <- clients.keys) {
      val percent = weight.toDouble / totalWeight
      // the tiny fudge fraction is added to counteract float errors.
      val itemWeight = (percent * serverCount * (NUM_REPS / 4) + 0.0000000001).toInt
      for (k <- 0 until itemWeight) {
        val key = if (port == 11211) {
          hostname + "-" + k
        } else {
          hostname + ":" + port + "-" + k
        }
        for (i <- 0 until 4) {
          continuum += computeHash(key, i) -> (hostname, port, weight)
        }
      }
    }

    assert(continuum.size <= NUM_REPS * serverCount)
    assert(continuum.size >= NUM_REPS * (serverCount - 1))

    continuum
  }

  protected[memcached] def clientOf(key: String) = {
    val hash = keyHasher.hashKey(key)
    val entry = continuum.ceilingEntry(hash)
    val clientTuple =
      if (entry ne null)
        entry.getValue
      else
        continuum.firstEntry.getValue
    val client = clients(clientTuple)
    client
  }

  protected def computeHash(key: String, alignment: Int) = {
    val hasher = MessageDigest.getInstance("MD5")
    hasher.update(key.getBytes("utf-8"))
    val buffer = ByteBuffer.wrap(hasher.digest)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.position(alignment << 2)
    buffer.getInt.toLong & 0xffffffffL
  }
}

case class KetamaClientBuilder(
  _nodes: Seq[(String, Int, Int)],
  _hashName: Option[String],
  _clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]]) {

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


/**
 * Ruby memcache-client (MemCache) compatible client.
 */
class RubyMemCacheClient(clients: Seq[Client]) extends PartitionedClient {
  protected[memcached] def clientOf(key: String) = {
    val hash = (KeyHasher.CRC32_ITU.hashKey(key) >> 16) & 0x7fff
    val index = hash % clients.size
    clients(index.toInt)
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
