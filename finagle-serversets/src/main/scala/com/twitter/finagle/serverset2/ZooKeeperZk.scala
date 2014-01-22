package com.twitter.finagle.serverset2

import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Future, Throw, Return, Promise, Duration}
import com.twitter.io.Buf
import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{ZooKeeper, KeeperException, Watcher}
import scala.collection.JavaConverters._

/**
 * An implementation of Zk using an underlying 
 * Apache ZooKeeper constructor.
 */
private class ZooKeeperZk(newZk: Watcher => ZooKeeper) extends Zk {
  import Zk.newWatcher
  
  protected[serverset2] implicit val timer = DefaultTimer.twitter

  private[this] val watcher = newWatcher()
  private[this] val zk = newZk(watcher)
  val state = watcher.state
  zk.register(watcher)

  private def determine[T](rc: Int, r: => T) = KeeperException.Code.get(rc) match {
    case KeeperException.Code.OK => Return(r)
    case err => Throw(KeeperException.create(err))
  }

  def exists(path: String) = {
    val p = new Promise[Option[Stat]]
    val cb = new StatCallback {
      def processResult(rc: Int, path: String, ctx: Object, stat: Stat) {
        if (rc == KeeperException.Code.NONODE.intValue())
          p() = Return(None)
        else
          p() = determine(rc, Option(stat))
      }
    }
    zk.exists(path, false, cb, null)
    p
  }

  def existsWatch(path: String) = {
    val watcher = newWatcher()
    val p = new Promise[Watched[Option[Stat]]]
    val cb = new StatCallback {
      def processResult(rc: Int, path: String, ctx: Object, stat: Stat) {
        if (rc == KeeperException.Code.NONODE.intValue())
          p() = Return((None, watcher.state))
        else
          p() = determine(rc, (Option(stat), watcher.state))
      }
    }
    zk.exists(path, watcher, cb, null)
    p
  }

  def getChildren(path: String) = {
    val p = new Promise[Seq[String]]
    val cb = new ChildrenCallback {
      def processResult(rc: Int, path: String, ctx: Object, children: java.util.List[String]) {
        val r = if (children == null) Seq() else children.asScala
        p() = determine(rc, r)
      }
    }
    zk.getChildren(path, false, cb, null)
    p
  }

  def getChildrenWatch(path: String) = {
    val p = new Promise[Watched[Seq[String]]]
    val watcher = newWatcher()
    val cb = new ChildrenCallback {
      def processResult(rc: Int, path: String, ctx: Object, children: java.util.List[String]) {
        val r = if (children == null) Seq() else children.asScala
        p() = determine(rc, (r, watcher.state))
      }
    }
    zk.getChildren(path, watcher, cb, null)
    p
  }

  def getData(path: String) = {
    val p = new Promise[(Stat, Buf)]
    val cb = new DataCallback {
      def processResult(rc: Int, path: String, ctx: Object, data: Array[Byte], stat: Stat) {
        p() = determine(rc, (stat, Buf.ByteArray(data)))
      }
    }
    zk.getData(path, false, cb, null)
    p
  }

  def getDataWatch(path: String) = {
    val p = new Promise[Watched[(Stat, Buf)]]
    val watcher = newWatcher()
    val cb = new DataCallback {
      def processResult(
          rc: Int, path: String, 
          ctx: Object, data: Array[Byte], 
          stat: Stat) {
        p() = determine(rc, ((stat, Buf.ByteArray(data)), watcher.state))
      }
    }
    zk.getData(path, watcher, cb, null)
    p
  }

  def sync(path: String) = {
    val p = new Promise[Unit]
    val cb = new VoidCallback {
      def processResult(rc: Int, path: String, ctx: Object) {
        p() = determine(rc, ())
      }
    }
    zk.sync(path, cb, null)
    p
  }

  def close(): Future[Unit] =
    // TODO: do we need to do this in a pool? It might be nice to
    // synthesize some events here. Or maybe define a special watch
    // event for closed.
    Future(zk.close())

  def sessionId: Long = zk.getSessionId
  def sessionPasswd: Buf = zk.getSessionPasswd match {
    case null => Buf.Empty
    case bytes => Buf.ByteArray(bytes)
  }
  def sessionTimeout: Duration = 
    Duration.fromMilliseconds(zk.getSessionTimeout)

  override def toString = "Zk(%s)".format(zk)
}


