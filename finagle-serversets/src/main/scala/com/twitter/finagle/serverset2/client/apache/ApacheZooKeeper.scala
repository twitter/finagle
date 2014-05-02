package com.twitter.finagle.serverset2.client.apache

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.finagle.serverset2.client._
import org.apache.zookeeper
import org.apache.zookeeper.AsyncCallback._
import scala.collection.JavaConverters._

/**
 * ZooKeeperClient implementation based on Apache ZooKeeper Library
 *
 * Provides Reader and Writer.
 * No Multi support.
 *
 * @param zk Underlying Apache ZooKeeper client.
 */
private[serverset2] class ApacheZooKeeper private[apache](zk: zookeeper.ZooKeeper)
    extends ZooKeeperRW {
  private def fromZKData(data: Array[Byte]): Option[Buf] = data match {
    case null => None
    case x => Some(Buf.ByteArray(x))
  }

  /**
   * ZooKeeper differentiates between an empty byte array and a null reference.
   *
   * @param data Option[Buf]
   * @return Array[Byte] or null
   */
  private def zkData(data: Option[Buf]): Array[Byte] = data match {
    case Some(b) => toByteArray(b)
    case None => null
  }

  def sessionId: Long = zk.getSessionId

  def sessionPasswd: Buf = Buf.ByteArray(zk.getSessionPasswd)

  def sessionTimeout: Duration = Duration.fromMilliseconds(zk.getSessionTimeout)

  def addAuthInfo(scheme: String, auth: Buf): Future[Unit] =
    Future.value(zk.addAuthInfo(scheme, toByteArray(auth)))

  override def close(deadline: Time): Future[Unit] = FuturePool.interruptibleUnboundedPool {
    zk.close()
  }

  def create(
      path: String,
      data: Option[Buf],
      acl: Seq[Data.ACL],
      createMode: CreateMode): Future[String] = {
    val rv = new Promise[String]
    val cb = new StringCallback {
      def processResult(ret: Int, path: String, ctx: Object, name: String) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(name)
          case Some(e) => rv.setException(e)
        }
    }
    zk.create(
      path,
      zkData(data),
      (acl map ApacheData.ACL.zk).asJava,
      ApacheCreateMode.zk(createMode),
      cb,
      null)
    rv
  }

  def delete(path: String, version: Option[Int]): Future[Unit] = {
    val rv = new Promise[Unit]
    val cb = new VoidCallback {
      def processResult(ret: Int, path: String, ctx: Object) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(Unit)
          case Some(e) => rv.setException(e)
        }
    }
    zk.delete(path, version getOrElse -1, cb, null)
    rv
  }

  def exists(path: String): Future[Option[Data.Stat]] = {
    val rv = new Promise[Option[Data.Stat]]
    val cb = new StatCallback {
      def processResult(ret: Int, path: String, ctx: Object, stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(Some(ApacheData.Stat(stat)))
          case Some(KeeperException.NoNode(_)) => rv.setValue(None)
          case Some(e) => rv.setException(e)
        }
    }
    zk.exists(path, null, cb, null)
    rv
  }

  def existsWatch(path: String): Future[Watched[Option[Data.Stat]]] = {
    val watcher = new ApacheWatcher
    val rv = new Promise[Watched[Option[Data.Stat]]]
    val cb = new StatCallback {
      def processResult(ret: Int, path: String, ctx: Object, stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(Watched(Some(ApacheData.Stat(stat)), watcher.state))
          case Some(KeeperException.NoNode(_)) => rv.setValue(Watched(None, watcher.state))
          case Some(e) => rv.setException(e)
        }
    }
    zk.exists(path, watcher, cb, null)
    rv
  }

  def getData(path: String): Future[Node.Data] = {
    val rv = new Promise[Node.Data]
    val cb = new DataCallback {
      def processResult(
          ret: Int,
          path: String,
          ctx: Object,
          data: Array[Byte],
          stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(Node.Data(fromZKData(data), ApacheData.Stat(stat)))
          case Some(e) => rv.setException(e)
        }
    }
    zk.getData(path, null, cb, null)
    rv
  }

  def getDataWatch(path: String): Future[Watched[Node.Data]] = {
    val watcher = new ApacheWatcher
    val rv = new Promise[Watched[Node.Data]]
    val cb = new DataCallback {
      def processResult(
          ret: Int,
          path: String,
          ctx: Object,
          data: Array[Byte],
          stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(
            Watched(Node.Data(fromZKData(data), ApacheData.Stat(stat)), watcher.state))
          case Some(e) => rv.setException(e)
        }
    }
    zk.getData(path, watcher, cb, null)
    rv
  }

  def setData(path: String, data: Option[Buf], version: Option[Int]): Future[Data.Stat] = {
    val rv = new Promise[Data.Stat]
    val cb = new StatCallback {
      def processResult(ret: Int, path: String, ctx: Object, stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(ApacheData.Stat(stat))
          case Some(e) => rv.setException(e)
        }
    }
    zk.setData(path, zkData(data), version getOrElse -1, cb, null)
    rv
  }

  def getACL(path: String): Future[Node.ACL] = {
    val rv = new Promise[Node.ACL]
    val cb = new ACLCallback {
      def processResult(
          ret: Int,
          path: String,
          ctx: Object,
          acl: java.util.List[zookeeper.data.ACL],
          stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(
            Node.ACL(acl.asScala.toList map (ApacheData.ACL(_)), ApacheData.Stat(stat)))
          case Some(e) => rv.setException(e)
        }
    }
    zk.getACL(path, null, cb, null)
    rv
  }

  def setACL(path: String, acl: Seq[Data.ACL], version: Option[Int]): Future[Data.Stat] = {
    val rv = new Promise[Data.Stat]
    val cb = new StatCallback {
      def processResult(ret: Int, path: String, ctx: Object, stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(ApacheData.Stat(stat))
          case Some(e) => rv.setException(e)
        }
    }
    zk.setACL(path, (acl map ApacheData.ACL.zk).asJava, version getOrElse -1, cb, null)
    rv
  }

  def getChildren(path: String): Future[Node.Children] = {
    val rv = new Promise[Node.Children]
    val cb = new Children2Callback {
      def processResult(
          ret: Int,
          path: String,
          ctx: Object,
          children: java.util.List[String],
          stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(Node.Children(children.asScala, ApacheData.Stat(stat)))
          case Some(e) => rv.setException(e)
        }
    }
    zk.getChildren(path, null, cb, null)
    rv
  }

  def getChildrenWatch(path: String): Future[Watched[Node.Children]] = {
    val watcher = new ApacheWatcher
    val rv = new Promise[Watched[Node.Children]]
    val cb = new Children2Callback {
      def processResult(
          ret: Int,
          path: String,
          ctx: Object,
          children: java.util.List[String],
          stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(
            Watched(Node.Children(children.asScala, ApacheData.Stat(stat)), watcher.state))
          case Some(e) => rv.setException(e)
        }
    }
    zk.getChildren(path, watcher, cb, null)
    rv
  }

  def globPrefixWatch(pat: String): Future[Watched[Seq[String]]] = {
    val (path, prefix) = try ZooKeeperReader.patToPathAndPrefix(pat) catch {
      case NonFatal(exc) => return Future.exception(exc)
    }

    val watcher = new ApacheWatcher
    val rv = new Promise[Watched[Seq[String]]]
    val cb = new Children2Callback {
      def processResult(
          ret: Int,
          path: String,
          ctx: Object,
          children: java.util.List[String],
          stat: zookeeper.data.Stat) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => 
            val paths = ZooKeeperReader.processGlob(path, prefix, children)
            rv.setValue(Watched(paths, watcher.state))
          case Some(e) => rv.setException(e)
        }
    }
    zk.getChildren(path, watcher, cb, null)
    rv
  }

  def sync(path: String): Future[Unit] = {
    val rv = new Promise[Unit]
    val cb = new VoidCallback {
      def processResult(ret: Int, path: String, ctx: Object) =
        ApacheKeeperException(ret, Option(path)) match {
          case None => rv.setValue(Unit)
          case Some(e) => rv.setException(e)
        }
    }
    zk.sync(path, cb, null)
    rv
  }

  override def toString: String = zk.toString
}

private[serverset2] object ApacheZooKeeper {
  /**
   * Create a new ZooKeeper client from a ClientConfig.
   *
   * @param config
   * @return a Watched[ZooKeeperRW]
   */
  private[apache] def newClient(config: ClientConfig): Watched[ZooKeeperRW] = {
    val timeoutInMs = config.sessionTimeout.inMilliseconds.toInt
    val watcher = new ApacheWatcher(config.statsReceiver)
    val zk = (config.sessionId, config.password) match {
      case (Some(id), Some(pw)) =>
        new ApacheZooKeeper(
          new zookeeper.ZooKeeper(config.hosts, timeoutInMs, watcher, id, toByteArray(pw)))
      case _ => new ApacheZooKeeper(new zookeeper.ZooKeeper(config.hosts, timeoutInMs, watcher))
    }
    val wrappedZk: ZooKeeperRW = new StatsRW {
      protected val underlying: ZooKeeperRW = zk
      protected val stats: StatsReceiver = config.statsReceiver
    }
    if (com.twitter.finagle.serverset2.client.chatty()) {
      val logger = Logger.get(getClass)
      Watched(
        new ChattyRW {
          protected val underlying: ZooKeeperRW = wrappedZk
          protected val print = { m: String => logger.info(m) }
        },
        watcher.state)
    }
    else
      Watched(wrappedZk, watcher.state)
  }
}

private class ApacheFactory extends ClientFactory[ZooKeeperRW] {
  val capabilities: Seq[Capability] = Seq(Reader, Writer)
  val priority: Int = 0

  def newClient(config: ClientConfig): Watched[ZooKeeperRW] =
    ApacheZooKeeper.newClient(config)
}
