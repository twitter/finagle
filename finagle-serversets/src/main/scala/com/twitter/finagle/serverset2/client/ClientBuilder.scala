package com.twitter.finagle.serverset2.client

import com.twitter.conversions.time._
import com.twitter.util.Duration
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.io.Buf
import com.twitter.finagle.util.LoadService
import java.net.InetSocketAddress

private[serverset2] sealed trait ClientFactory[+T <: ZooKeeperClient] {
  def newClient(config: ClientConfig): Watched[T]
}

private[serverset2] trait ReaderFactory extends ClientFactory[ZooKeeperReader]
private[serverset2] trait WriterFactory extends ReaderFactory with ClientFactory[ZooKeeperRW]
private[serverset2] trait MultiFactory extends WriterFactory with ClientFactory[ZooKeeperRWMulti]

private[client] case class ClientConfig(
    val hosts: String,
    val sessionTimeout: Duration,
    val statsReceiver: StatsReceiver,
    val readOnlyOK: Boolean,
    val sessionId: Option[Long],
    val password: Option[Buf])
{
  def toMap = Map(
    "hosts" -> hosts,
    "sessionTimeout" -> sessionTimeout,
    "statsReceiver" -> statsReceiver,
    "readOnlyOK" -> readOnlyOK,
    "sessionId" -> sessionId,
    "password" -> password
  )

  override def toString = {
    "ClientConfig(%s)".format(
      toMap flatMap {
        case (k, Some(v)) =>
          Some("%s=%s".format(k, v))
        case _ =>
          None
      } mkString(", "))
  }
}

/**
 * Build a ZooKeeper Client.
 *
 * Configure:
 *
 * hosts(hosts) sets a list of ZooKeeper hosts:ports to connect to ["localhost:2181"]
 * sessionTimeout(timeout) sets ZooKeeper session timeout [10.seconds]
 * statsReceiver(receiver) sets Stats Receiver for client stats. [DefaultStatsReceiver]
 * readOnlyOK() enables read-only support from disconnected observers. [False]
 * sessionId(id) sets session ID for reconnection. [None]
 * password(pwd) sets session Password for reconnection. [None]
 *
 * Build:
 *
 * reader() builds Read Only client.
 * writer() builds Read/Write client.
 * multi() builds Read/Write/Multi client.
 */
private[serverset2] object ClientBuilder {
  private val DefaultConfig: ClientConfig = ClientConfig(
    hosts = "localhost:2181",
    sessionTimeout = 10.seconds,
    statsReceiver = DefaultStatsReceiver,
    readOnlyOK = false,
    sessionId = None,
    password = None
  )

  def apply() = new ClientBuilder(DefaultConfig)

  /**
   * Used for Java access.
   */
  def get() = apply()
}

private[client] class ClientBuilder(
    config: ClientConfig) {
  private def resolve[T](fs: Seq[T]) = fs match {
    case Seq() => throw new RuntimeException("No ZooKeeper ClientFactory Found")
    case Seq(f) => f
    case many => throw new RuntimeException("Multiple ZooKeeper ClientFactories Available")
  }

  override def toString() = "ClientBuilder(%s)".format(config.toString)

  protected def copy(config: ClientConfig): ClientBuilder = new ClientBuilder(config)

  protected def withConfig(f: ClientConfig => ClientConfig): ClientBuilder = copy(f(config))

  /**
   * Create a new ZooKeeperReader client.
   *
   * @return configured Watched[ZooKeeperReader]
   * @throws RuntimeException if no matching, or multiple ReaderFactories are found.
   */
  def reader(): Watched[ZooKeeperReader] = resolve(LoadService[ReaderFactory]()).newClient(config)

  /**
   * Create a new ZooKeeperRW client.
   *
   * @return configured Watched[ZooKeeperRW]
   * @throws RuntimeException if no matching, or multiple WriterFactories are found.
   */
  def writer(): Watched[ZooKeeperRW] =
    resolve(LoadService[WriterFactory]()).newClient(config)

  /**
   * Create a new ZooKeeperRWMulti client.
   *
   * @return configured Watched[ZooKeeperRWMulti]
   * @throws RuntimeException if no matching, or multiple MultiFactories are found.
   */
  def multi(): Watched[ZooKeeperRWMulti] =
    resolve(LoadService[MultiFactory]()).newClient(config)

  /**
   * Configure builder with list of ZooKeeper servers in an ensemble.
   *
   * @param zkHosts comma separated host:port list.
   * @return configured ClientBuilder
   */
  def hosts(zkHosts: String): ClientBuilder = withConfig(_.copy(hosts = zkHosts))

  /**
   * Configure builder with list of ZooKeeper servers in an ensemble.
   *
   * @param zkHosts sequence of InetSocketAddresses.
   * @return configured ClientBuilder
   */
  def hosts(zkHosts: Seq[InetSocketAddress]): ClientBuilder =
    hosts(zkHosts map { h => "%s:%d,".format(h.getHostName, h.getPort) } mkString)

  /**
   * Configure builder with a session timeout.
   *
   * @param zkTimeout duration.
   * @return configured ClientBuilder
   */
  def sessionTimeout(zkTimeout: Duration): ClientBuilder =
    withConfig(_.copy(sessionTimeout = zkTimeout))

  /**
   * Configure builder with a Stats Receiver.
   *
   * @param statsIn stats receiver.
   * @return configured ClientBuilder
   */
  def statsReceiver(statsIn: StatsReceiver): ClientBuilder =
    withConfig(_.copy(statsReceiver = statsIn))

  /**
   * Enable read-only support from disconnected observers.
   *
   * This feature is not supported by all implementations.
   *
   * When a ZooKeeper ensemble loses quorum, observers may choose to allow
   * clients to continue to read data, which may be out of date. This is
   * NOT advised for clients that desire consistency over availability.
   *
   * @return configured ClientBuilder
   */
  def readOnlyOK(): ClientBuilder =
    withConfig(_.copy(readOnlyOK = true))

  /**
   * Configure builder with a session ID for reconnection.
   *
   * @param sessionId existing session ID.
   * @return configured ClientBuilder
   */
  def sessionId(sessionId: Long): ClientBuilder =
    withConfig(_.copy(sessionId = Some(sessionId)))

  /**
   * Configure builder with a session password for reconnection.
   *
   * @param password existing session password.
   * @return configured ClientBuilder
   */
  def password(password: Buf): ClientBuilder =
    withConfig(_.copy(password = Some(password)))
}
