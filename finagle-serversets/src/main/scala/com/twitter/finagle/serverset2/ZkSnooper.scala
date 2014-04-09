package com.twitter.finagle.serverset2

import com.twitter.util.{Duration, Future, Var, Timer}
import com.twitter.io.Buf
import org.apache.zookeeper.data.Stat

/**
 * A debugging tool to see what low-level ZK
 * commands are being issued, together with
 * their responses.
 */
private[serverset2] class ZkSnooper(self: Zk, print: String => Unit) extends Zk {
  protected[serverset2] implicit val timer: Timer = self.timer

  private[this] def printOp[U](name: String, op: => Future[U], args: String*): Future[U] = {
    print("->"+name+"("+(args mkString ",")+")")
    op respond { t =>
      print("<-"+name+"("+(args mkString ",")+") = "+t)
    }
  }

  def exists(path: String): Future[Option[Stat]] =
    printOp("exists", self.exists(path), path)

  def existsWatch(path: String): Future[Watched[Option[Stat]]] =
    printOp("existsWatch", self.existsWatch(path), path)

  def getChildren(path: String): Future[Seq[String]] = 
    printOp("getChildren", self.getChildren(path), path)

  def getChildrenWatch(path: String): Future[Watched[Seq[String]]] =
    printOp("getChildrenWatch", self.getChildrenWatch(path), path)

  def getData(path: String): Future[(Stat, Buf)] =
    printOp("getData", self.getData(path), path)

  def getDataWatch(path: String): Future[Watched[(Stat, Buf)]] =
    printOp("getDataWatch", self.getDataWatch(path), path)

  def sync(path: String): Future[Unit] =
    printOp("sync", self.sync(path), path)

  def close(): Future[Unit] =
    printOp("close", self.close())

  def sessionId: Long = self.sessionId
  def sessionPasswd: Buf = self.sessionPasswd
  def sessionTimeout: Duration = self.sessionTimeout
  val state: Var[WatchState] = self.state
}
