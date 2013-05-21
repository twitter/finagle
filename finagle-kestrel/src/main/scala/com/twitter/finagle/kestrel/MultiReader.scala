package com.twitter.finagle.kestrel

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.conversions.time._
import com.twitter.finagle.builder._
import com.twitter.finagle.kestrel.protocol.{Response, Command, Kestrel}
import com.twitter.util.{Duration, Timer}
import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.net.SocketAddress
import _root_.java.{util => ju}
import scala.collection.JavaConversions._

object AllHandlesDiedException extends Exception

/**
 * Read from multiple clients in round-robin fashion, "grabby hands"
 * style.  The load balancing is simple, and falls out naturally from
 * the user of the {{Offer}} mechanism: When there are multiple
 * available messages, round-robin across them.  Otherwise, wait for
 * the first message to arrive.
 *
 * Cluster example:
 * <code>
 *   Cluster[SocketAddress] cluster = ... // e.g. ZookeeperServerSetCluster
 *   val readHandle =
 *     MultiReader(cluster, "the-queue")
 *       .clientBuilder(
 *         ClientBuilder()
 *           .codec(Kestrel())
 *           .requestTimeout(1.minute)
 *           .connectTimeout(1.minute)
 *           .hostConnectionLimit(1) /* etc... but do not set hosts or build */)
 *       .retryBackoffs(/* Stream[Duration], Timer; optional */)
 *       .build()
 * </code>
 */
object MultiReader {
  def apply(cluster: Cluster[SocketAddress], queueName: String): ClusterMultiReaderBuilder = {
    val config = ClusterMultiReaderConfig(cluster, queueName)
    new ClusterMultiReaderBuilder(config)
  }

  def newBuilder(cluster: Cluster[SocketAddress], queueName: String) = apply(cluster, queueName)

  @deprecated("Use Cluster-based apply", "5.3.7")
  def apply(clients: Seq[Client], queueName: String): ReadHandle =
    apply(clients map { _.readReliably(queueName) })

  /**
   * A java friendly interface: we use scala's implicit conversions to
   * feed in a {{java.util.Iterator<ReadHandle>}}
   */
  @deprecated("Use Cluster-based apply", "5.3.7")
  def apply(handles: ju.Iterator[ReadHandle]): ReadHandle =
    apply(handles.toSeq)


  @deprecated("Use Cluster-based apply", "5.3.7")
  def apply(handles: Seq[ReadHandle]): ReadHandle = {
    val cluster = new StaticCluster[ReadHandle](handles)
    merge(cluster)
  }

  def merge(readHandleCluster: Cluster[ReadHandle]): ReadHandle = {
    val error = new Broker[Throwable]
    val messages = new Broker[ReadMessage]
    val close = new Broker[Unit]
    val clusterUpdate = new Broker[Cluster.Change[ReadHandle]]

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
      val errors = handles.map { h => h.error  map { _ => h } }.toSeq
      val closeOf = close.recv { _ => onClose(handles) }

      // we sequence here to ensure that close gets
      // priority over reads
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
          clusterUpdate.recv {
            case Cluster.Add(handle) => loop(handles + handle)
            case Cluster.Rem(handle) =>
              handle.close()
              loop(handles - handle)
          }
        )
      }

      of.sync()
    }


    for(() <- readHandleCluster.ready) {
      val (current, spoolFuture) = readHandleCluster.snap
      loop(Set() ++ current)
      spoolFuture foreach { spool =>
        spool foreach { update => clusterUpdate ! update }
      }
    }

    ReadHandle(messages.recv, error.recv, close.send(()))
  }
}

final case class ClusterMultiReaderConfig private[kestrel](
  private val _cluster: Cluster[SocketAddress],
  private val _queueName: String,
  private val _clientBuilder: Option[ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]] = None,
  private val _timer: Option[Timer] = None,
  private val _retryBackoffs: Option[() => Stream[Duration]] = None) {

  // Delegators to make a friendly public API
  val cluster = _cluster
  val queueName = _queueName
  val clientBuilder = _clientBuilder
  val timer = _timer
  val retryBackoffs = _retryBackoffs
}

/**
 * Factory for [[com.twitter.finagle.kestrel.ReadHandle]] instances.
 */
class ClusterMultiReaderBuilder private[kestrel](config: ClusterMultiReaderConfig) {

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

  private[this] def buildReadHandleCluster(): Cluster[ReadHandle] = {
    val baseClientBuilder = config.clientBuilder match {
      case Some(clientBuilder) => clientBuilder
      case None =>
        ClientBuilder()
          .codec(Kestrel())
          .connectTimeout(1.minute)
          .requestTimeout(1.minute)
          .hostConnectionLimit(1)
    }

    config.cluster map { socketAddr =>
      val factory = baseClientBuilder
        .hosts(socketAddr)
        .buildFactory()

      val client = Client(factory)

      (config.retryBackoffs, config.timer) match {
        case (Some(backoffs), Some(timer)) => client.readReliably(config.queueName, timer, backoffs())
        case _                             => client.readReliably(config.queueName)
      }
    }
  }

  /*
   * Contructs a merged ReadHandle over the members of the configured cluster. The handle is updated
   * as members are added or removed.
   */
  def build() = MultiReader.merge(buildReadHandleCluster())
}
