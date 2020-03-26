package com.twitter.finagle.zookeeper

import com.twitter.finagle.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.finagle.common.zookeeper.{ServerSet, ServerSetImpl, ZooKeeperClient}
import com.twitter.finagle.{Announcer, Announcement}
import com.twitter.util.{Future, Promise}
import java.net.InetSocketAddress
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
 * Indicates that a failure occurred while attempting to announce the server
 * using a [[com.twitter.finagle.zookeeper.ZkAnnouncer]].
 */
class ZkAnnouncerException(msg: String) extends Exception(msg)

/**
 * When announcing an endpoint with additional endpoints they all need to join
 * the path together. However, announcements are decoupled and can happen in any
 * order. The ZkAnnouncer will gather endpoints together in order to announce
 * them as a group. Main endpoints are required and are thus announced immediately
 * while additional endpoints are only announced if the path also has a main
 * endpoint. For this reason if a main endpoint is announced first and an additional
 * endpoint announced later, the announcer must leave the path and re-announce. The
 * process is similar: leave the path, then remove either the additional endpoint
 * or the main endpoint, re-join only if the main endpoint exists. Also it is also
 *
 * @note Requiring the shardId in some of the announce methods is an unfortunate artifact of the
 *       implementation of ServerSets. For most uses setting it to 0 is sufficient.
 * @note announcing multiple endpoints can happen in two different ways. One by calling announce
 *       separately for each endpoint as mentioned above, and another by calling announce and
 *       passing additionalEndpoints to it directly in the same method call.
 */
class ZkAnnouncer(factory: ZkClientFactory) extends Announcer { self =>
  val scheme = "zk"

  def this() = this(DefaultZkClientFactory)

  private[this] case class ServerSetConf(
    client: ZooKeeperClient,
    path: String,
    shardId: Int,
    serverSet: ServerSet,
    var status: Option[EndpointStatus] = None,
    var addr: Option[InetSocketAddress] = None,
    metadata: Map[String, String] = Map.empty,
    endpoints: mutable.Map[String, InetSocketAddress] =
      mutable.Map.empty[String, InetSocketAddress])

  private[this] case class Mutation(
    conf: ServerSetConf,
    addr: Option[InetSocketAddress],
    endpoints: Map[String, InetSocketAddress],
    onComplete: Promise[Unit])

  private[this] val emptyMetadata = Map.empty[String, String]
  private[this] var serverSets = Set.empty[ServerSetConf]
  private[this] val q = new LinkedBlockingQueue[Mutation]()
  private[this] val mutator = new Thread("ZkAnnouncer Mutator") {
    setDaemon(true)
    start()

    override def run(): Unit = {
      while (true) {
        val change = q.take()
        try {
          val conf = change.conf

          conf.status foreach { status =>
            status.leave()
            conf.status = None
          }

          change.addr foreach { addr =>
            conf.status = Some(
              conf.serverSet
                .join(addr, change.endpoints.asJava, conf.shardId, conf.metadata.asJava))
          }

          change.onComplete.setDone()
        } catch {
          case NonFatal(e) => change.onComplete.setException(e)
        }
      }
    }
  }

  private[this] def doChange(conf: ServerSetConf): Future[Unit] = {
    val onComplete = new Promise[Unit]
    q.offer(Mutation(conf, conf.addr, conf.endpoints.toMap, onComplete))
    onComplete
  }

  def announce(
    hosts: String,
    path: String,
    shardId: Int,
    addr: InetSocketAddress,
    endpoint: Option[String]
  ): Future[Announcement] =
    announce(hosts, path, shardId, addr, endpoint, emptyMetadata)

  def announce(
    hosts: String,
    path: String,
    shardId: Int,
    addr: InetSocketAddress,
    endpoint: Option[String],
    metadata: Map[String, String]
  ): Future[Announcement] =
    announce(hosts, path, shardId, addr, endpoint, metadata, Map.empty[String, InetSocketAddress])

  /**
   * @param additionalEndpoints if this is non-empty these endpoints are announced along with the
   *                            primary addr
   */
  def announce(
    hosts: String,
    path: String,
    shardId: Int,
    addr: InetSocketAddress,
    endpoint: Option[String],
    metadata: Map[String, String],
    additionalEndpoints: Map[String, InetSocketAddress]
  ): Future[Announcement] = {
    val zkHosts = factory.hostSet(hosts)
    if (zkHosts.isEmpty)
      Future.exception(
        new ZkAnnouncerException("ZK client address \"%s\" resolves to nothing".format(hosts))
      )
    else
      announce(
        factory.get(zkHosts)._1,
        path,
        shardId,
        addr,
        endpoint,
        metadata,
        additionalEndpoints)
  }

  def announce(
    client: ZooKeeperClient,
    path: String,
    shardId: Int,
    addr: InetSocketAddress,
    endpoint: Option[String]
  ): Future[Announcement] = {
    announce(
      client,
      path,
      shardId,
      addr,
      endpoint,
      emptyMetadata,
      Map.empty[String, InetSocketAddress])
  }

  def announce(
    client: ZooKeeperClient,
    path: String,
    shardId: Int,
    addr: InetSocketAddress,
    endpoint: Option[String],
    metadata: Map[String, String]
  ): Future[Announcement] = {
    announce(client, path, shardId, addr, endpoint, metadata, Map.empty[String, InetSocketAddress])
  }

  /**
   * @param additionalEndpoints if this is non-empty these endpoints are announced along with the
   *                            primary addr
   */
  def announce(
    client: ZooKeeperClient,
    path: String,
    shardId: Int,
    addr: InetSocketAddress,
    endpoint: Option[String],
    metadata: Map[String, String],
    additionalEndpoints: Map[String, InetSocketAddress]
  ): Future[Announcement] = {
    val conf = serverSets find { s =>
      s.client == client && s.path == path && s.shardId == shardId
    } getOrElse {
      val serverSetConf =
        ServerSetConf(client, path, shardId, new ServerSetImpl(client, path), metadata = metadata)
      synchronized { serverSets += serverSetConf }
      serverSetConf
    }

    conf.synchronized {
      endpoint match {
        case Some(ep) => conf.endpoints.put(ep, addr)
        case None => conf.addr = Some(addr)
      }

      conf.endpoints ++= additionalEndpoints

      doChange(conf) map { _ =>
        new Announcement {
          def unannounce() = {
            conf.synchronized {
              endpoint match {
                case Some(ep) => conf.endpoints.remove(ep)
                case None => conf.addr = None
              }
              doChange(conf)
            }
          }
        }
      }
    }
  }

  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] =
    announce(ia, addr, emptyMetadata)

  def announce(
    ia: InetSocketAddress,
    addr: String,
    metadata: Map[String, String]
  ): Future[Announcement] =
    announce(ia, addr, metadata, Map.empty[String, InetSocketAddress])

  /**
   * @param additionalEndpoints if this is non-empty these endpoints are announced along with the
   *                            primary addr
   */
  def announce(
    ia: InetSocketAddress,
    addr: String,
    metadata: Map[String, String],
    additionalEndpoints: Map[String, InetSocketAddress]
  ): Future[Announcement] =
    addr.split("!") match {
      // zk!host!/full/path!shardId
      case Array(hosts, path, shardId) =>
        announce(hosts, path, shardId.toInt, ia, None, metadata, additionalEndpoints)

      // zk!host!/full/path!shardId!endpoint
      case Array(hosts, path, shardId, endpoint) =>
        announce(hosts, path, shardId.toInt, ia, Some(endpoint), metadata, additionalEndpoints)

      case _ =>
        Future.exception(new ZkAnnouncerException("Invalid addr \"%s\"".format(addr)))
    }
}
