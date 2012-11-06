package com.twitter.finagle.memcached

import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable}

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import _root_.java.net.{SocketAddress, InetSocketAddress}
import _root_.java.util.{Map => JMap}

import com.google.common.base.{CharMatcher, Strings}

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.builder.{Cluster, ClientBuilder, ClientConfig, StaticCluster}
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.service.{FailureAccrualFactory, FailedService}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{Service, ShardNotAvailableException}
import com.twitter.hashing._
import com.twitter.util.{Time, Future, Bijection, Duration, Timer}

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
      .codec(Memcached())
      .build())

  /**
   * Construct a client from a Cluster
   */
  def apply(cluster: Cluster[SocketAddress]) : Client = Client(
    ClientBuilder()
      .cluster(cluster)
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
  /**
   * Equivalaent to results.reduceLeft { _ ++ _ }, but written to be more efficient.
   */
  private[memcached] def merged(results: Seq[GetResult]): GetResult = {
    results match {
      case Nil => GetResult()
      case Seq(single) => single
      case Seq(a, b) => a ++ b
      case _ =>
        val hits = new mutable.HashMap[String, Value]
        val misses = new mutable.HashSet[String]
        val failures = new mutable.HashMap[String, Throwable]

        for (result <- results) {
          hits ++= result.hits
          misses ++= result.misses
          failures ++= result.failures
        }

        GetResult(hits.toMap, misses.toSet, failures.toMap)
    }
  }

  private[memcached] def merged(results: Seq[GetsResult]): GetsResult = {
    val unwrapped = results map { _.getResult }
    GetsResult(merged(unwrapped))
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
   */
  def cas(
    key: String, flags: Int, expiry: Time, value: T, casUnique: ChannelBuffer
  ): Future[JBoolean]

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
  def delete(key: String): Future[JBoolean]

  /**
   * Increment a key. Interpret the value as an Long if it is parsable.
   * This operation has no effect if there is no value there already.
   */
  def incr(key: String, delta: Long): Future[Option[JLong]]
  def incr(key: String): Future[Option[JLong]] = incr(key, 1L)

  /**
   * Decrement a key. Interpret the value as an JLong if it is parsable.
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
   * Perform a CAS operation on the key, only if the value has not
   * changed since the value was last retrieved, and `casUnique`
   * extracted from a `gets` command.  We treat the "cas unique" token
   * opaquely, but in reality it is a string-encoded u64.
   *
   * @return true if replaced, false if not
   */
  def cas(key: String, value: T, casUnique: ChannelBuffer): Future[JBoolean] =
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
 * @param  service  the underlying Memcached Service.
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

  def getResult(keys: Iterable[String]) = {
    try {
      if (keys==null) throw new IllegalArgumentException("Invalid keys: keys cannot be null")
      rawGet(Get(keys.toSeq))
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }
  def getsResult(keys: Iterable[String]) = {
    try {
      if (keys==null) throw new IllegalArgumentException("Invalid keys: keys cannot be null")
      rawGet(Gets(keys.toSeq)) map { GetsResult(_) }
    }  catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) = {
    try {
      service(Set(key, flags, expiry, value)) map {
        case Stored() => ()
        case Error(e) => throw e
        case _        => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def cas(key: String, flags: Int, expiry: Time, value: ChannelBuffer, casUnique: ChannelBuffer) = {
    try {
      service(Cas(key, flags, expiry, value, casUnique)) map {
        case Stored() => true
        case Exists() => false
        case Error(e) => throw e
        case _        => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer) = {
    try {
      service(Add(key, flags, expiry, value)) map {
        case Stored()     => true
        case NotStored()  => false
        case Error(e)     => throw e
        case _            => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer) = {
    try {
      service(Append(key, flags, expiry, value)) map {
        case Stored()     => true
        case NotStored()  => false
        case Error(e)     => throw e
        case _            => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer) = {
    try {
      service(Prepend(key, flags, expiry, value)) map {
        case Stored()     => true
        case NotStored()  => false
        case Error(e)     => throw e
        case _            => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer) = {
    try {
      service(Replace(key, flags, expiry, value)) map {
        case Stored()     => true
        case NotStored()  => false
        case Error(e)     => throw e
        case _            => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def delete(key: String) = {
    try {
      service(Delete(key)) map {
        case Deleted()    => true
        case NotFound()   => false
        case Error(e)     => throw e
        case _            => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def incr(key: String, delta: Long): Future[Option[JLong]] = {
    try {
      service(Incr(key, delta)) map {
        case Number(value) => Some(value)
        case NotFound()    => None
        case Error(e)      => throw e
        case _             => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
    }
  }

  def decr(key: String, delta: Long): Future[Option[JLong]] = {
    try {
      service(Decr(key, delta)) map {
        case Number(value) => Some(value)
        case NotFound()    => None
        case Error(e)      => throw e
        case _             => throw new IllegalStateException
      }
    } catch {
      case t:IllegalArgumentException => Future.exception(new ClientError(t.getMessage))
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
  private[memcached] def identifier = if (port == 11211) host else host + ":" + port
}

private[memcached] sealed trait NodeEvent
private[memcached] case class NodeJoin(key: KetamaClientKey, service: Service[Command, Response]) extends NodeEvent
private[memcached] case class NodeLeave(key: KetamaClientKey) extends NodeEvent
private[memcached] sealed trait NodeHealth extends NodeEvent
private[memcached] case class NodeMarkedDead(key: KetamaClientKey) extends NodeHealth
private[memcached] case class NodeRevived(key: KetamaClientKey) extends NodeHealth

class KetamaFailureAccrualFactory[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  numFailures: Int,
  markDeadFor: Duration,
  timer: Timer,
  key: KetamaClientKey,
  healthBroker: Broker[NodeHealth]
) extends FailureAccrualFactory[Req, Rep](underlying, numFailures, markDeadFor, timer) {

  override def markDead() = {
    super.markDead()
    healthBroker ! NodeMarkedDead(key)
  }

  override def revive() = {
    super.revive()
    healthBroker ! NodeRevived(key)
  }
}

object KetamaClient {
  val NumReps = 160
  private val shardNotAvailableDistributor = {
    val failedService = new FailedService(new ShardNotAvailableException)
    new SingletonDistributor(Client(failedService))
  }
}

class KetamaClient private[memcached](
  initialServices: Map[KetamaClientKey, Service[Command, Response]],
  nodeChange: Offer[NodeEvent],
  keyHasher: KeyHasher,
  numReps: Int,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  oldLibMemcachedVersionComplianceMode: Boolean = false
) extends PartitionedClient {
  private object NodeState extends Enumeration {
    type t = this.Value
    val Live, Ejected = Value
  }
  private case class Node(node: KetamaNode[Client], var state: NodeState.Value)

  private[this] val nodes = mutable.Map[KetamaClientKey, Node]() ++ {
    initialServices map { case (clientKey, service) =>
      clientKey -> Node(KetamaNode(clientKey.identifier, clientKey.weight, Client(service)), NodeState.Live)
    }
  }
  private[this] val pristineDistributor = buildDistributor(nodes.values map(_.node) toSeq)
  @volatile private[this] var currentDistributor: Distributor[Client] = pristineDistributor

  private[this] val liveNodeGauge = statsReceiver.addGauge("live_nodes") {
    synchronized { nodes count { case (_, Node(_, state)) => state == NodeState.Live } } }
  private[this] val deadNodeGauge = statsReceiver.addGauge("dead_nodes") {
    synchronized { nodes count { case (_, Node(_, state)) => state == NodeState.Ejected } } }
  private[this] val ejectionCount = statsReceiver.counter("ejections")
  private[this] val revivalCount = statsReceiver.counter("revivals")
  private[this] val nodeLeaveCount = statsReceiver.counter("leaves")
  private[this] val nodeJoinCount = statsReceiver.counter("joins")
  private[this] val keyRingRedistributeCount = statsReceiver.counter("redistributes")

  private[this] def buildDistributor(nodes: Seq[KetamaNode[Client]]) =
    if (nodes.isEmpty) KetamaClient.shardNotAvailableDistributor
    else new KetamaDistributor(nodes, numReps, oldLibMemcachedVersionComplianceMode)

  nodeChange foreach {
    case NodeMarkedDead(key) =>
      ejectNode(key)
      ejectionCount.incr()
    case NodeRevived(key) =>
      reviveNode(key)
      revivalCount.incr()
    case NodeLeave(key) =>
      removeNode(key)
      nodeLeaveCount.incr()
    case NodeJoin(key, client) =>
      joinNode(key, client)
      nodeJoinCount.incr()
  }

  override def clientOf(key: String): Client = {
    val hash = keyHasher.hashKey(key)
    currentDistributor.nodeForHash(hash)
  }

  private[this] def rebuildDistributor(): Unit = synchronized {
    val liveNodes = for ((_, Node(node, NodeState.Live)) <- nodes) yield node
    currentDistributor = buildDistributor(liveNodes toSeq)
    keyRingRedistributeCount.incr()
  }

  private[this] def ejectNode(key: KetamaClientKey) = synchronized {
    val node = nodes(key)
    if (node.state == NodeState.Live) {
      node.state = NodeState.Ejected
      rebuildDistributor()
    }
  }

  private[this] def reviveNode(key: KetamaClientKey) = synchronized {
    val node = nodes(key)
    if (node.state == NodeState.Ejected) {
      node.state = NodeState.Live
      rebuildDistributor()
    }
  }

  private[this] def removeNode(key: KetamaClientKey) = synchronized {
    nodes.remove(key) match {
      case Some(Node(node, _)) =>
        rebuildDistributor()
        node.handle.release()
      case _ =>
    }
  }

  private[this] def joinNode(key: KetamaClientKey, service: Service[Command, Response]): Unit = synchronized {
    if (nodes contains key)
      return ()

    nodes(key) = Node(KetamaNode(key.identifier, key.weight, Client(service)), NodeState.Live)
    rebuildDistributor()
  }

  def release() = synchronized {
    for ((_, Node(node, _)) <- nodes)
      node.handle.release()
  }
}

case class KetamaClientBuilder private[memcached] (
  _cluster: Cluster[CacheNode],
  _hashName: Option[String],
  _clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]],
  _numFailures: Int = 5,
  _markDeadFor: Duration = 30.seconds,
  oldLibMemcachedVersionComplianceMode: Boolean = false
) {

  def cluster(cluster: Cluster[InetSocketAddress]): KetamaClientBuilder = {
    // TODO: for now, we assume all the cluster created this way has equal weight.
    copy(
      _cluster = cluster map {
        socketAddr =>
          new CacheNode(socketAddr.getHostName, socketAddr.getPort, 1)
      }
    )
  }

  def cachePoolCluster(cluster: Cluster[CacheNode]): KetamaClientBuilder = {
    copy(_cluster = cluster)
  }

  def nodes(nodes: Seq[(String, Int, Int)]): KetamaClientBuilder =
    copy(_cluster = CachePoolCluster.newStaticCluster(nodes map {
      case (host, port, weight) =>
        new CacheNode(host, port, weight)
    } toSet))

  def nodes(hostPortWeights: String): KetamaClientBuilder =
    nodes(PartitionedClient.parseHostPortWeights(hostPortWeights))

  def hashName(hashName: String): KetamaClientBuilder =
    copy(_hashName = Some(hashName))

  def clientBuilder(clientBuilder: ClientBuilder[_, _, _, _, ClientConfig.Yes]): KetamaClientBuilder =
    copy(_clientBuilder = Some(clientBuilder))

  def failureAccrualParams(numFailures: Int, markDeadFor: Duration): KetamaClientBuilder =
    copy(_numFailures = numFailures, _markDeadFor = markDeadFor)

  def enableOldLibMemcachedVersionComplianceMode(): KetamaClientBuilder =
    copy(oldLibMemcachedVersionComplianceMode = true)

  def build(): Client = {
    val builder =
      (_clientBuilder getOrElse ClientBuilder().hostConnectionLimit(1)).codec(Memcached())

    // A dedicated broker for both cache node health event and cache pool change event.
    val nodeChangeBroker = new Broker[NodeEvent]

    def notifyNodeJoin(node: CacheNode) {
      val key = KetamaClientKey(node.host, node.port, node.weight)
      val (fa, health) = failureAccrualWrapper(key)
      val client = builder
          .hosts(new InetSocketAddress(key.host, key.port))
          .failureAccrualFactory(fa)
          .build()
      nodeChangeBroker ! new NodeJoin(key, client)
      health foreach { healthEvent =>
        nodeChangeBroker ! healthEvent
      }
    }

    def notifyNodeLeave(node: CacheNode) {
      nodeChangeBroker ! new NodeLeave(KetamaClientKey(node.host, node.port, node.weight))
    }

    val (cacheNodes, clusterUpdates) = _cluster.snap
    cacheNodes foreach { notifyNodeJoin(_) }
    clusterUpdates foreach { spool =>
      spool foreach {
        case Cluster.Add(node) => notifyNodeJoin(node)
        case Cluster.Rem(node) => notifyNodeLeave(node)
      }
    }

    val keyHasher = KeyHasher.byName(_hashName.getOrElse("ketama"))
    val statsReceiver = builder.statsReceiver.scope("memcached_client")

    new KetamaClient(
      Map(),
      nodeChangeBroker.recv,
      keyHasher,
      KetamaClient.NumReps,
      statsReceiver,
      oldLibMemcachedVersionComplianceMode
    )
  }

  private[this] def filter(key: KetamaClientKey, broker: Broker[NodeHealth])(timer: Timer) = new ServiceFactoryWrapper {
    def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]) = {
        new KetamaFailureAccrualFactory(
          factory, _numFailures, _markDeadFor, timer, key, broker
        )
      }
  }

  private[this] def failureAccrualWrapper(key: KetamaClientKey) = {
    val broker = new Broker[NodeHealth]
    (filter(key, broker) _, broker.recv)
  }
}

object KetamaClientBuilder {
  def apply(): KetamaClientBuilder = KetamaClientBuilder(new StaticCluster(Nil), Some("ketama"), None)
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
      Client(builder.hosts(hostname + ":" + port).codec(Memcached()).build())
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
      val client = Client(builder.hosts(hostname + ":" + port).codec(Memcached()).build())
      for (i <- (1 to weight)) yield client
    }.flatten.toArray
    new PHPMemCacheClient(clients, keyHasher)
  }
}

object PHPMemCacheClientBuilder {
  def apply(): PHPMemCacheClientBuilder = PHPMemCacheClientBuilder(Nil, Some("crc32-itu"), None)
  def get() = apply()
}
