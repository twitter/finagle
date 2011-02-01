package com.twitter.finagle.memcached

import _root_.java.util.TreeMap
import _root_.java.security.MessageDigest
import _root_.java.nio.{ByteBuffer, ByteOrder}

import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConversions._
import com.twitter.finagle.builder.ClientBuilder
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
      .codec(new Memcached)
      .build())

  /**
   * Construct a partitioned client from a set of Services.
   */
  def apply(services: Seq[Service[Command, Response]]): Client = {
    val clients = services.map(apply(_))
    val hasher = KeyHasher.byName("fnv")
    val circle = {
      val circle = new TreeMap[Long, Client]()
      clients foreach { client =>
        circle += hasher.hashKey(client.toString) -> client
      }
      circle
    }
    new PartitionedClient(clients, circle, hasher)
  }

  /**
   * Construct an unpartitioned client from a single Service.
   */
  def apply(raw: Service[Command, Response]): Client = {
    new ConnectedClient(raw)
  }
}

/**
 * A friendly client to talk to a Memcached server.
 */
trait Client {
  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer):     Future[Response]
  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer):     Future[Response]
  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer):  Future[Response]
  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[Response]
  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[Response]

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
   */
  def delete(key: String):                        Future[Response]

  /**
   * Increment a key. Interpret the key as an integer if it is parsable.
   * This operation has no effect if there is no value there already.
   * A common idiom is to set(key, ""), incr(key).
   */
  def incr(key: String):                          Future[Int]
  def incr(key: String, delta: Int):              Future[Int]

  /**
   * Decrement a key. Interpret the key as an integer if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  def decr(key: String):                          Future[Int]
  def decr(key: String, delta: Int):              Future[Int]

  /**
   * Store a key. Override an existing values.
   */
  def set(key: String, value: ChannelBuffer):     Future[Response] = set(key, 0, Time.epoch, value)

  /**
   * Store a key but only if it doesn't already exist on the server.
   */
  def add(key: String, value: ChannelBuffer):     Future[Response] = add(key, 0, Time.epoch, value)

  /**
   * Append a set of bytes to the end of an existing key. If the key doesn't
   * exist, the operation has no effect.
   */
  def append(key: String, value: ChannelBuffer):  Future[Response] = append(key, 0, Time.epoch, value)

  /**
   * Prepend a set of bytes to the beginning of an existing key. If the key
   * doesn't exist, the operation has no effect.
   */
  def prepend(key: String, value: ChannelBuffer): Future[Response] = prepend(key, 0, Time.epoch, value)

  /**
   * Replace an item if it exists. If it doesn't exist, the operation has no
   * effect.
   */
  def replace(key: String, value: ChannelBuffer): Future[Response] = replace(key, 0, Time.epoch, value)
}

/**
 * A Client connected to an individual Memcached server.
 *
 * @param  underlying  the underlying Memcached Service.
 */
protected class ConnectedClient(underlying: Service[Command, Response]) extends Client {
  def get(key: String) = {
    underlying(Get(Seq(key))) map {
      case Values(values) =>
        if (values.size > 0) Some(values.head.value)
        else None
    }
  }

  def get(keys: Iterable[String]) = {
    underlying(Get(keys.toSeq)) map {
      case Values(values) =>
        val tuples = values.map {
          case Value(key, value) =>
            (key.toString(CharsetUtil.UTF_8), value)
        }
        Map(tuples: _*)
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Set(key, flags, expiry, value))

  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Add(key, flags, expiry, value))

  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Append(key, flags, expiry, value))

  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Prepend(key, flags, expiry, value))

  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Replace(key, flags, expiry, value))

  def delete(key: String)                        = underlying(Delete(key))

  def incr(key: String): Future[Int]             = incr(key, 1)

  def decr(key: String): Future[Int]             = decr(key, 1)

  def incr(key: String, delta: Int): Future[Int] = {
    underlying(Incr(key, delta)) map {
      case Number(value) =>
        value
    }
  }

  def decr(key: String, delta: Int): Future[Int] = {
    underlying(Decr(key, delta)) map {
      case Number(value) =>
        value
    }
  }

  override def toString = hashCode.toString // FIXME this incompatible with Ketama
}

/**
 * A Memcached client that partitions data across multiple servers according to a
 * the provided consistent hash ring and key hasher.
 *
 * @param ring      consistent hash ring
 * @param keyHasher hash function for hashing keys on the hash ring
 */
class PartitionedClient(clients: Seq[Client], ring: TreeMap[Long, Client], keyHasher: KeyHasher) extends Client {
  require(clients.size > 0, "At least one client must be provided")

  def get(key: String)                    = idx(key).get(key)
  def get(keys: Iterable[String])         = {
    val keysGroupedByClient = keys.groupBy(idx(_))

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
  }

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).set(key, flags, expiry, value)
  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).add(key, flags, expiry, value)
  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).append(key, flags, expiry, value)
  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).prepend(key, flags, expiry, value)
  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).replace(key, flags, expiry, value)

  def delete(key: String)                        = idx(key).delete(key)
  def incr(key: String)                          = idx(key).incr(key)
  def incr(key: String, delta: Int)              = idx(key).incr(key, delta)
  def decr(key: String)                          = idx(key).decr(key)
  def decr(key: String, delta: Int)              = idx(key).decr(key, delta)

  private[this] def idx(key: String) = {
    val entry = ring.ceilingEntry(keyHasher.hashKey(key))
    val client = if (entry ne null) entry.getValue
    else ring.firstEntry.getValue
    client
  }
}

case class KetamaClientBuilder(
  _nodes: Seq[Tuple3[String, Int, Int]],
  _hashName: Option[String],
  _clientBuilder: Option[ClientBuilder[Command, Response]]) {

  def this() = this(
    Nil,  // nodes
    None, // hashName
    None  // clientBuilder
  )

  def nodes(nodes: Seq[Tuple3[String, Int, Int]]): KetamaClientBuilder =
    copy(_nodes = nodes)

  def nodes(hostPortWeights: String): KetamaClientBuilder =
    copy(_nodes = parseHostPortWeights(hostPortWeights))

  def hashName(hashName: String): KetamaClientBuilder =
    copy(_hashName = Some(hashName))

  def clientBuilder(clientBuilder: ClientBuilder[Command, Response]): KetamaClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def parseHostPortWeights(hostPortWeights: String): Seq[Tuple3[String, Int, Int]] = {
    val hpws = hostPortWeights split Array(' ', ',') filter (_ != "") map(_.split(":"))
    hpws map { hpw => (hpw(0), hpw(1).toInt, hpw(2).toInt) } toList
  }

  private[this] def computeHash(key: String, alignment: Int) = {
    val hasher = MessageDigest.getInstance("MD5")
    hasher.update(key.getBytes("utf-8"))
    val buffer = ByteBuffer.wrap(hasher.digest)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.position(alignment << 2)
    buffer.getInt.toLong & 0xffffffffL
  }

  def build(): PartitionedClient = {
    val builder = _clientBuilder getOrElse ClientBuilder()
    val hasher = KeyHasher.byName(_hashName getOrElse "fnv")
    var continuum = new TreeMap[Long, Client]()
    var clients: Seq[Client] = Seq()

    val serverCount = _nodes.size
    val totalWeight = _nodes.foldLeft(0.0) {_+_._3}

    // we use (NUM_REPS * #servers) total points, but allocate them based on server weights.
    val NUM_REPS = 160

    for ((hostname, port, weight) <- _nodes) {
      val client = Client(builder.hosts(hostname + ":" + port).codec(new Memcached).build())
      clients = clients :+ client

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
          continuum += computeHash(key, i) -> client
        }
      }
    }

    assert(continuum.size <= NUM_REPS * serverCount)
    assert(continuum.size >= NUM_REPS * (serverCount - 1))

    new PartitionedClient(clients, continuum, hasher)
  }
}
