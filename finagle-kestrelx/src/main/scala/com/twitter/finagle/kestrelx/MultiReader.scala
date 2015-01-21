package com.twitter.finagle.kestrelx

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, Group, Name, ServiceFactory}
import com.twitter.finagle.builder._
import com.twitter.finagle.kestrelx.protocol.{Response, Command, Kestrel}
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ClientId, ThriftClientRequest}
import com.twitter.util._
import _root_.java.{util => ju}
import _root_.java.lang.UnsupportedOperationException
import _root_.java.util.logging.Level
import _root_.java.net.SocketAddress
import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * Indicates that all [[com.twitter.finagle.kestrelx.ReadHandle ReadHandles]]
 * that are backing a given [[com.twitter.finagle.kestrelx.MultiReader]] have
 * died.
 */
object AllHandlesDiedException extends Exception

private[finagle] object MultiReaderHelper {
  private[finagle] def merge(readHandles: Var[Try[Set[ReadHandle]]]): ReadHandle = {
    val error = new Broker[Throwable]
    val messages = new Broker[ReadMessage]
    val close = new Broker[Unit]
    val clusterUpdate = new Broker[Set[ReadHandle]]

    def onClose(handles: Set[ReadHandle]) {
      handles foreach { _.close() }
      error ! ReadClosedException
    }

    def loop(handles: Set[ReadHandle]) {
      if (handles.isEmpty) {
        error ! AllHandlesDiedException
        return
      }

      val queues = handles.map { _.messages }.toSeq
      val errors = handles.map { h => h.error map { _ => h } }.toSeq
      val closeOf = close.recv { _ => onClose(handles) }

      // We sequence here to ensure that `close` gets priority over reads.
      Offer.prioritize(
        closeOf,
        Offer.choose(queues:_*) { m =>
          messages ! m
          loop(handles)
        },
        Offer.choose(errors:_*) { h =>
          h.close()
          loop(handles - h)
        },
        clusterUpdate.recv { newHandles =>
        // Close any handles that exist in old set but not the new one.
          (handles &~ newHandles) foreach { _.close() }
          loop(newHandles)
        }
      ).sync()
    }

    // Wait until the ReadHandles set is populated before initializing.
    val readHandlesPopulatedFuture = readHandles.changes.collect[Try[Set[ReadHandle]]] {
      case r@Return(x) if x.nonEmpty => r
    }.toFuture()

    val closeWitness: Future[Closable] = readHandlesPopulatedFuture flatMap {
      // Flatten the Future[Try[T]] to Future[T].
      Future.const
    } map { handles =>
    // Once the cluster is non-empty, start looping and observing updates.
      loop(handles)

      // Send cluster updates on the appropriate broker.
      val witness = Witness { tsr: Try[Set[ReadHandle]] =>
        synchronized {
          tsr match {
            case Return(newHandles) => clusterUpdate !! newHandles
            case Throw(t) => error !! t
          }
        }
      }

      readHandles.changes.register(witness)
    }

    val closeHandleOf: Offer[Unit] = close.send(()) map { _ =>
      closeWitness onSuccess { _.close() }
    }

    ReadHandle(messages.recv, error.recv, closeHandleOf)
  }
}

/**
 * Read from multiple clients in round-robin fashion, "grabby hands"
 * style using Kestrel's memcache protocol.  The load balancing is simple,
 * and falls out naturally from the user of the {{Offer}} mechanism: When
 * there are multiple available messages, round-robin across them.  Otherwise,
 * wait for the first message to arrive.
 *
 * Var[Addr] example:
 * {{{
 *   val name: com.twitter.finagle.Name = Resolver.eval(...)
 *   val va: Var[Addr] = name.bind()
 *   val readHandle =
 *     MultiReaderMemcache(va, "the-queue")
 *       .clientBuilder(
 *         ClientBuilder()
 *           .codec(MultiReaderMemcache.codec)
 *           .requestTimeout(1.minute)
 *           .connectTimeout(1.minute)
 *           .hostConnectionLimit(1) /* etc... but do not set hosts or build */)
 *       .retryBackoffs(/* Stream[Duration], Timer; optional */)
 *       .build()
 * }}}
 */
object MultiReaderMemcache {
  def apply(dest: Name, queueName: String): MultiReaderBuilderMemcache = {
    dest match {
      case Name.Bound(va) => apply(va, queueName)
      case Name.Path(_) => throw new UnsupportedOperationException(
        "Failed to bind Name.Path in `MultiReaderMemcache.apply`"
      )
    }
  }

  def apply(va: Var[Addr], queueName: String): MultiReaderBuilderMemcache = {
    val config = MultiReaderConfig[Command, Response](va, queueName)
    new MultiReaderBuilderMemcache(config)
  }

  /**
   * Helper for getting the right codec for the memcache protocol
   * @return the Kestrel codec
   */
  def codec = Kestrel()
}

/**
 * Read from multiple clients in round-robin fashion, "grabby hands"
 * style using Kestrel's memcache protocol.  The load balancing is simple,
 * and falls out naturally from the user of the {{Offer}} mechanism: When
 * there are multiple available messages, round-robin across them.  Otherwise,
 * wait for the first message to arrive.
 *
 * Example with a custom client builder:
 * {{{
 *   val name: com.twitter.finagle.Name = Resolver.eval(...)
 *   val va: Var[Addr] = name.bind()
 *   val readHandle =
 *     MultiReaderThrift(va, "the-queue")
 *       .clientBuilder(
 *         ClientBuilder()
 *           .codec(MultiReaderThrift.codec(ClientId("myClientName"))
 *           .requestTimeout(1.minute)
 *           .connectTimeout(1.minute)
 *           .hostConnectionLimit(1) /* etc... but do not set hosts or build */)
 *       .retryBackoffs(/* Stream[Duration], Timer; optional */)
 *       .build()
 * }}}
 *
 * Example without a customer client builder so clientId passed to apply
 * {{{
 *   val name: com.twitter.finagle.Name = Resolver.eval(...)
 *   val va: Var[Addr] = name.bind()
 *   val readHandle =
 *     MultiReaderThrift(va, "the-queue", ClientId("myClientName"))
 *       .retryBackoffs(/* Stream[Duration], Timer; optional */)
 *       .build()
 * }}}
 */
object MultiReaderThrift {
  /**
   * Used to create a thrift based MultiReader with a ClientId when a custom
   * client builder will not be used.  If a custom client builder will be
   * used then it is more reasonable to use the version of apply that does
   * not take a ClientId or else the client id will need to be passed to
   * both apply and the codec in clientBuilder.
   * @param dest a [[com.twitter.finagle.Name]] representing the Kestrel
   * endpoints to connect to
   * @param queueName the name of the queue to read from
   * @param clientId the clientid to be used
   * @return A MultiReaderBuilderThrift
   */
  def apply(dest: Name, queueName: String, clientId: Option[ClientId]): MultiReaderBuilderThrift = {
    dest match {
      case Name.Bound(va) => apply(va, queueName, clientId)
      case Name.Path(_) => throw new UnsupportedOperationException(
        "Failed to bind Name.Path in `MultiReaderThrift.apply`"
      )
    }
  }

  /**
   * Used to create a thrift based MultiReader with a ClientId when a custom
   * client builder will not be used.  If a custom client builder will be
   * used then it is more reasonable to use the version of apply that does
   * not take a ClientId or else the client id will need to be passed to
   * both apply and the codec in clientBuilder.
   * @param va endpoints for Kestrel
   * @param queueName the name of the queue to read from
   * @param clientId the clientid to be used
   * @return A MultiReaderBuilderThrift
   */
  def apply(
    va: Var[Addr],
    queueName: String,
    clientId: Option[ClientId]
  ): MultiReaderBuilderThrift = {
    val config = MultiReaderConfig[ThriftClientRequest, Array[Byte]](va, queueName, clientId)
    new MultiReaderBuilderThrift(config)
  }

  /**
   * Used to create a thrift based MultiReader when a ClientId will neither
   * not be provided or will be provided to the codec was part of creating
   * a custom client builder.
   * This is provided as a separate method for Java compatability.
   * @param va endpoints for Kestrel
   * @param queueName the name of the queue to read from
   * @return A MultiReaderBuilderThrift
   */
  def apply(va: Var[Addr], queueName: String): MultiReaderBuilderThrift = {
    this(va,queueName, None)
  }

  /**
   * Helper for getting the right codec for the thrift protocol
   * @return the ThriftClientFramedCodec codec
   */
  def codec(clientId: ClientId) = ThriftClientFramedCodec(Some(clientId))
}

/**
 * Read from multiple clients in round-robin fashion, "grabby hands"
 * style.  The load balancing is simple, and falls out naturally from
 * the user of the {{Offer}} mechanism: When there are multiple
 * available messages, round-robin across them.  Otherwise, wait for
 * the first message to arrive.
 *
 * Var[Addr] example:
 * {{{
 *   val name: com.twitter.finagle.Name = Resolver.eval(...)
 *   val va: Var[Addr] = name.bind()
 *   val readHandle =
 *     MultiReader(va, "the-queue")
 *       .clientBuilder(
 *         ClientBuilder()
 *           .codec(Kestrel())
 *           .requestTimeout(1.minute)
 *           .connectTimeout(1.minute)
 *           .hostConnectionLimit(1) /* etc... but do not set hosts or build */)
 *       .retryBackoffs(/* Stream[Duration], Timer; optional */)
 *       .build()
 * }}}
 */
@deprecated("Use MultiReaderMemcache or MultiReaderThrift instead", "6.15.1")
object MultiReader {
  /**
   * Create a Kestrel memcache protocol based builder
   */
  @deprecated("Use MultiReaderMemcache.apply instead", "6.15.1")
  def apply(va: Var[Addr], queueName: String): ClusterMultiReaderBuilder = {
    val config = ClusterMultiReaderConfig(va, queueName)
    new ClusterMultiReaderBuilder(config)
  }

  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def apply(cluster: Cluster[SocketAddress], queueName: String): ClusterMultiReaderBuilder = {
    val Name.Bound(va) = Name.fromGroup(Group.fromCluster(cluster))
    apply(va, queueName)
  }

  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def apply(clients: Seq[Client], queueName: String): ReadHandle =
    apply(clients map { _.readReliably(queueName) })

  /**
   * A java friendly interface: we use scala's implicit conversions to
   * feed in a {{java.util.Iterator<ReadHandle>}}
   */
  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def apply(handles: ju.Iterator[ReadHandle]): ReadHandle =
    MultiReaderHelper.merge(Var.value(Return(handles.toSet)))


  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def apply(handles: Seq[ReadHandle]): ReadHandle =
    MultiReaderHelper.merge(Var.value(Return(handles.toSet)))

  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def newBuilder(cluster: Cluster[SocketAddress], queueName: String) = apply(cluster, queueName)

  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def merge(readHandleCluster: Cluster[ReadHandle]): ReadHandle = {
    val varTrySet = Group.fromCluster(readHandleCluster).set map { Try(_) }
    MultiReaderHelper.merge(varTrySet)
  }
}

/**
 * Multi reader configuration settings
 */
final case class MultiReaderConfig[Req, Rep] private[kestrelx](
  private val _va: Var[Addr],
  private val _queueName: String,
  private val _clientId: Option[ClientId] = None,
  private val _txnAbortTimeout: Duration = Duration.Top,
  private val _clientBuilder:
    Option[ClientBuilder[Req, Rep, Nothing, ClientConfig.Yes, ClientConfig.Yes]] = None,
  private val _timer: Option[Timer] = None,
  private val _retryBackoffs: Option[() => Stream[Duration]] = None) {

  // Delegators to make a friendly public API
  val va = _va
  val queueName = _queueName
  val clientBuilder = _clientBuilder
  val timer = _timer
  val retryBackoffs = _retryBackoffs
  val clientId = _clientId
  val txnAbortTimeout = _txnAbortTimeout
}

@deprecated("Use MultiReaderConfig[Req, Rep] instead", "6.15.1")
final case class ClusterMultiReaderConfig private[kestrelx](
  private val _va: Var[Addr],
  private val _queueName: String,
  private val _clientBuilder:
    Option[ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]] = None,
  private val _timer: Option[Timer] = None,
  private val _retryBackoffs: Option[() => Stream[Duration]] = None) {

  // Delegators to make a friendly public API
  val va = _va
  val queueName = _queueName
  val clientBuilder = _clientBuilder
  val timer = _timer
  val retryBackoffs = _retryBackoffs

  /**
   * Convert to MultiReaderConfig[Command, Response] during deprecation
   */
  def toMultiReaderConfig: MultiReaderConfig[Command, Response] = {
    MultiReaderConfig[Command, Response](
      this.va,
      this.queueName,
      None,
      Duration.Top,
      this.clientBuilder,
      this.timer,
      this.retryBackoffs)
  }
}

/**
 * Factory for [[com.twitter.finagle.kestrelx.ReadHandle]] instances.
 */
abstract class MultiReaderBuilder[Req, Rep, Builder] private[kestrelx](
  config: MultiReaderConfig[Req, Rep]) {
  type ClientBuilderBase = ClientBuilder[Req, Rep, Nothing, ClientConfig.Yes, ClientConfig.Yes]

  private[this] val ReturnEmptySet = Return(Set.empty[ReadHandle])

  protected[kestrelx] def copy(config: MultiReaderConfig[Req, Rep]): Builder

  protected[kestrelx] def withConfig(
      f: MultiReaderConfig[Req, Rep] => MultiReaderConfig[Req, Rep]): Builder = {
    copy(f(config))
  }

  protected[kestrelx] def defaultClientBuilder: ClientBuilderBase

  protected[kestrelx] def createClient(factory: ServiceFactory[Req, Rep]): Client

  /**
   * Specify the ClientBuilder used to generate client objects. <b>Do not</b> specify the
   * hosts or cluster on the given ClientBuilder, and <b>do not</b> invoke <code>build()</code>
   * on it. You must specify a codec and host
   * connection limit, however.
   */
  def clientBuilder(clientBuilder: ClientBuilderBase): Builder =
    withConfig(_.copy(_clientBuilder = Some(clientBuilder)))

  /**
   * Specify the stream of Durations and Timer used for retry backoffs.
   */
  def retryBackoffs(backoffs: () => Stream[Duration], timer: Timer): Builder =
    withConfig(_.copy(_retryBackoffs = Some(backoffs), _timer = Some(timer)))

  /**
   * Specify the clientId to use, if applicable, for the default builder.
   * If the default client builder is override using {{clientBuilder}} then
   * this clientId has not effect.
   */
  def clientId(clientId: ClientId): Builder =
    withConfig(_.copy(_clientId = Some(clientId)))

  private[this] def buildReadHandleVar(): Var[Try[Set[ReadHandle]]] = {
    val baseClientBuilder = config.clientBuilder match {
      case Some(clientBuilder) => clientBuilder
      case None => defaultClientBuilder
    }

    // Use a mutable Map so that we can modify it in-place on cluster change.
    val currentHandles = mutable.Map.empty[SocketAddress, ReadHandle]

    val event = config.va.changes map {
      case Addr.Bound(socketAddrs, _) => {
        val newHandles = (socketAddrs &~ currentHandles.keySet) map { socketAddr =>
          val factory = baseClientBuilder
            .hosts(socketAddr)
            .buildFactory()

          val client = createClient(factory)

          val handle = (config.retryBackoffs, config.timer) match {
            case (Some(backoffs), Some(timer)) =>
              client.readReliably(config.queueName, timer, backoffs())
            case _ => client.readReliably(config.queueName)
          }
          handle.error foreach { case NonFatal(cause) => factory.close() }
          (socketAddr, handle)
        }

        synchronized {
          currentHandles.retain { case (addr, _) => socketAddrs.contains(addr) }
          currentHandles ++= newHandles
        }

        Return(currentHandles.values.toSet)
      }

      case Addr.Failed(t) => Throw(t)

      case _ => ReturnEmptySet
    }

    Var(Return(Set.empty), event)
  }

  /**
   * Constructs a merged ReadHandle over the members of the configured cluster.
   * The handle is updated as members are added or removed.
   */
  def build(): ReadHandle = MultiReaderHelper.merge(buildReadHandleVar())
}

abstract class MultiReaderBuilderMemcacheBase[Builder] private[kestrelx](
    config: MultiReaderConfig[Command, Response])
  extends MultiReaderBuilder[Command, Response, Builder](config) {
  type MemcacheClientBuilder =
    ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]

  protected[kestrelx] def defaultClientBuilder: MemcacheClientBuilder =
    ClientBuilder()
      .codec(Kestrel())
      .connectTimeout(1.minute)
      .requestTimeout(1.minute)
      .hostConnectionLimit(1)
      .daemon(true)

  protected[kestrelx] def createClient(factory: ServiceFactory[Command, Response]): Client =
    Client(factory)
}

@deprecated("Use MultiReaderBuilderMemcache instead", "6.15.1")
class ClusterMultiReaderBuilder private[kestrelx](config: ClusterMultiReaderConfig)
  extends MultiReaderBuilderMemcacheBase[ClusterMultiReaderBuilder](config.toMultiReaderConfig) {

  private def this(config: MultiReaderConfig[Command, Response]) = this(
    ClusterMultiReaderConfig(config.va, config.queueName, config.clientBuilder, config.timer))

  protected[kestrelx] def copy(
      config: MultiReaderConfig[Command, Response]): ClusterMultiReaderBuilder =
    new ClusterMultiReaderBuilder(config)

  protected[kestrelx] def copy(config: ClusterMultiReaderConfig): ClusterMultiReaderBuilder =
    new ClusterMultiReaderBuilder(config)

  protected[kestrelx] def withConfig(
      f: ClusterMultiReaderConfig => ClusterMultiReaderConfig): ClusterMultiReaderBuilder = {
    copy(f(config))
  }
}

/**
 * Factory for [[com.twitter.finagle.kestrelx.ReadHandle]] instances using
 * Kestrel's memcache protocol.
 */
class MultiReaderBuilderMemcache private[kestrelx](config: MultiReaderConfig[Command, Response])
  extends MultiReaderBuilderMemcacheBase[MultiReaderBuilderMemcache](config) {

  protected[kestrelx] def copy(
      config: MultiReaderConfig[Command, Response]): MultiReaderBuilderMemcache =
    new MultiReaderBuilderMemcache(config)
}

/**
 * Factory for [[com.twitter.finagle.kestrelx.ReadHandle]] instances using
 * Kestrel's thrift protocol.
 */
class MultiReaderBuilderThrift private[kestrelx](
    config: MultiReaderConfig[ThriftClientRequest, Array[Byte]])
  extends MultiReaderBuilder[ThriftClientRequest, Array[Byte], MultiReaderBuilderThrift](config) {
  type ThriftClientBuilder =
    ClientBuilder[ThriftClientRequest, Array[Byte], Nothing, ClientConfig.Yes, ClientConfig.Yes]

  protected[kestrelx] def copy(
      config: MultiReaderConfig[ThriftClientRequest, Array[Byte]]): MultiReaderBuilderThrift =
    new MultiReaderBuilderThrift(config)

  protected[kestrelx] def defaultClientBuilder: ThriftClientBuilder =
    ClientBuilder()
      .codec(ThriftClientFramedCodec(config.clientId))
      .connectTimeout(1.minute)
      .requestTimeout(1.minute)
      .hostConnectionLimit(1)
      .daemon(true)

  protected[kestrelx] def createClient(
      factory: ServiceFactory[ThriftClientRequest, Array[Byte]]): Client =
    Client.makeThrift(factory, config.txnAbortTimeout)

  /**
   * While reading items, an open transaction will be auto aborted if not confirmed by the client within the specified
   * timeout.
   */
  def txnAbortTimeout(txnAbortTimeout: Duration) =
    withConfig(_.copy(_txnAbortTimeout = txnAbortTimeout))
}

