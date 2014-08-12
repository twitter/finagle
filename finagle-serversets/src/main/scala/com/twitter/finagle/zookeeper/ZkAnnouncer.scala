package com.twitter.finagle.zookeeper

import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.common.zookeeper.{ServerSet, ServerSetImpl, ZooKeeperClient}
import com.twitter.finagle.{Announcer, Announcement}
import com.twitter.util.{Future, NonFatal, Promise}
import java.net.InetSocketAddress
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import scala.collection.JavaConverters._
import scala.collection.mutable

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
 * or the main endpoint, re-join only if the main endpoint exists.
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
    endpoints: mutable.Map[String, InetSocketAddress] = mutable.Map.empty[String, InetSocketAddress])

  private[this] case class Mutation(
    conf: ServerSetConf,
    addr: Option[InetSocketAddress],
    endpoints: Map[String, InetSocketAddress],
    onComplete: Promise[Unit])

  private[this] var serverSets = Set.empty[ServerSetConf]
  private[this] val q = new LinkedBlockingQueue[Mutation]()
  private[this] val mutator = new Thread("ZkAnnouncer Mutator") {
    setDaemon(true)
    start()

    override def run() {
      while (true) {
        val change = q.take()
        try {
          val conf = change.conf

          conf.status foreach { status =>
            status.leave()
            conf.status = None
          }

          change.addr foreach { addr =>
            conf.status = Some(conf.serverSet.join(addr, change.endpoints.asJava, conf.shardId))
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
  ): Future[Announcement] = {
    val zkHosts = factory.hostSet(hosts)
    if (zkHosts.isEmpty)
      Future.exception(new ZkAnnouncerException("ZK client address \"%s\" resolves to nothing".format(hosts)))
    else
      announce(factory.get(zkHosts)._1, path, shardId, addr, endpoint)
  }

  def announce(
    client: ZooKeeperClient,
    path: String,
    shardId: Int,
    addr: InetSocketAddress,
    endpoint: Option[String]
  ): Future[Announcement] = {
    val conf = serverSets find { s => s.client == client && s.path == path && s.shardId == shardId } getOrElse {
      val serverSetConf = ServerSetConf(client, path, shardId, new ServerSetImpl(client, path))
      synchronized { serverSets += serverSetConf }
      serverSetConf
    }

    conf.synchronized {
      endpoint match {
        case Some(ep) => conf.endpoints.put(ep, addr)
        case None => conf.addr = Some(addr)
      }

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

  /**
   * Requiring the shardId here is an unfortunate artifact of the implementation of ServerSets. For most uses
   * setting it to 0 is sufficient
   */
  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] =
    addr.split("!") match {
      // zk!host!/full/path!shardId
      case Array(hosts, path, shardId) =>
        announce(hosts, path, shardId.toInt, ia, None)

      // zk!host!/full/path!shardId!endpoint
      case Array(hosts, path, shardId, endpoint) =>
        announce(hosts, path, shardId.toInt, ia, Some(endpoint))

      case _ =>
        Future.exception(new ZkAnnouncerException("Invalid addr \"%s\"".format(addr)))
    }
}
