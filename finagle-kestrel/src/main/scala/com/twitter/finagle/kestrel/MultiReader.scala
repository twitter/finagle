package com.twitter.finagle.kestrel

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, Group, Name}
import com.twitter.finagle.builder._
import com.twitter.finagle.kestrel.protocol.{Response, Command, Kestrel}
import com.twitter.util.{Await, Closable, Duration, Future, Return, Throw, Try, Timer, Var, Witness}
import _root_.java.{util => ju}
import _root_.java.net.SocketAddress
import scala.collection.mutable
import scala.collection.JavaConversions._

object AllHandlesDiedException extends Exception

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
object MultiReader {
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
    merge(Var.value(Return(handles.toSet)))


  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def apply(handles: Seq[ReadHandle]): ReadHandle =
    merge(Var.value(Return(handles.toSet)))

  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def newBuilder(cluster: Cluster[SocketAddress], queueName: String) = apply(cluster, queueName)

  @deprecated("Use Var[Addr]-based `apply` method", "6.8.2")
  def merge(readHandleCluster: Cluster[ReadHandle]): ReadHandle = {
    val varTrySet = Group.fromCluster(readHandleCluster).set map { Try(_) }
    merge(varTrySet)
  }

  private[finagle] def merge(readHandles: Var[Try[Set[ReadHandle]]]): ReadHandle = {
    val error = new Broker[Throwable]
    val messages = new Broker[ReadMessage]
    val close = new Broker[Unit]
    val clusterUpdate = new Broker[Set[ReadHandle]]

    def onClose(handles: Set[ReadHandle]) {
      handles foreach { _.close }
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
      val of = closeOf orElse {
        Offer.choose(
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
        )
      }

      of.sync()
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

final case class ClusterMultiReaderConfig private[kestrel](
  private val _va: Var[Addr],
  private val _queueName: String,
  private val _clientBuilder: Option[ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]] = None,
  private val _timer: Option[Timer] = None,
  private val _retryBackoffs: Option[() => Stream[Duration]] = None) {

  // Delegators to make a friendly public API
  val va = _va
  val queueName = _queueName
  val clientBuilder = _clientBuilder
  val timer = _timer
  val retryBackoffs = _retryBackoffs
}

/**
 * Factory for [[com.twitter.finagle.kestrel.ReadHandle]] instances.
 */
class ClusterMultiReaderBuilder private[kestrel](config: ClusterMultiReaderConfig) {
  private[this] val ReturnEmptySet = Return(Set.empty[ReadHandle])

  protected def copy(config: ClusterMultiReaderConfig) = new ClusterMultiReaderBuilder(config)
  protected def withConfig(f: ClusterMultiReaderConfig => ClusterMultiReaderConfig): ClusterMultiReaderBuilder = {
    copy(f(config))
  }

  /*
   * Specify the ClientBuilder used to generate client objects. <b>Do not</b> specify the hosts or cluster on the
   * given ClientBuilder, and <b>do not</b> invoke <code>build()</code> on it. You must specify a codec and host
   * connection limit, however.
   */
  def clientBuilder(clientBuilder: ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]) = {
    withConfig(_.copy(_clientBuilder = Some(clientBuilder)))
  }

  /*
   * Specify the stream of Durations and Timer used for retry backoffs.
   */
  def retryBackoffs(backoffs: () => Stream[Duration], timer: Timer) = {
    withConfig(_.copy(_retryBackoffs = Some(backoffs), _timer = Some(timer)))
  }

  private[this] def buildReadHandleVar(): Var[Try[Set[ReadHandle]]] = {
    val baseClientBuilder = config.clientBuilder match {
      case Some(clientBuilder) => clientBuilder
      case None =>
        ClientBuilder()
          .codec(Kestrel())
          .connectTimeout(1.minute)
          .requestTimeout(1.minute)
          .hostConnectionLimit(1)
          .daemon(true)
    }

    // Use a mutable Map so that we can modify it in-place on cluster change.
    val currentHandles = mutable.Map.empty[SocketAddress, ReadHandle]

    val event = config.va.changes map {
      case Addr.Bound(socketAddrs) => {
        val newHandles = (socketAddrs &~ currentHandles.keySet) map { socketAddr =>
          val factory = baseClientBuilder
            .hosts(socketAddr)
            .buildFactory()

          val client = Client(factory)

          val handle = (config.retryBackoffs, config.timer) match {
            case (Some(backoffs), Some(timer)) =>
              client.readReliably(config.queueName, timer, backoffs())
            case _ => client.readReliably(config.queueName)
          }

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

  /*
   * Contructs a merged ReadHandle over the members of the configured cluster. The handle is updated
   * as members are added or removed.
   */
  def build(): ReadHandle = MultiReader.merge(buildReadHandleVar())
}
