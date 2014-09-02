package com.twitter.finagle.serverset2.client

import com.twitter.app.GlobalFlag
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future, Time}

private[serverset2] object chatty extends GlobalFlag(false, "Track ZooKeeper calls and responses")

/**
 * A tool to see what low-level ZK commands are being issued,
 *  together with their responses.
 */
private[serverset2] trait ChattyClient extends ZooKeeperClient {
  protected val underlying: ZooKeeperClient
  protected val print: String => Unit

  protected def printOp[U](name: String, op: => Future[U], args: String*): Future[U] = {
    print("->"+name+"("+(args mkString ",")+")")
    op respond { t =>
      print("<-"+name+"("+(args mkString ",")+") = "+t)
    }
  }

  def addAuthInfo(scheme: String, auth: Buf): Future[Unit] =
    printOp("addAuthInfo", underlying.addAuthInfo(scheme, auth), scheme, auth.toString)

  def close(deadline: Time): Future[Unit] =
    printOp("close", underlying.close(deadline))

  def getEphemerals(): Future[Seq[String]] = printOp("getEphemerals", underlying.getEphemerals())
  def sessionId: Long = underlying.sessionId
  def sessionPasswd: Buf = underlying.sessionPasswd
  def sessionTimeout: Duration = underlying.sessionTimeout
}

private[serverset2] trait ChattyReader extends ChattyClient with ZooKeeperReader {
  protected val underlying: ZooKeeperReader

  def exists(path: String): Future[Option[Data.Stat]] =
    printOp("exists", underlying.exists(path), path)

  def existsWatch(path: String): Future[Watched[Option[Data.Stat]]] =
    printOp("existsWatch", underlying.existsWatch(path), path)

  def getACL(path: String): Future[Node.ACL] =
    printOp("getACL", underlying.getACL(path), path)

  def getChildren(path: String): Future[Node.Children] =
    printOp("getChildren", underlying.getChildren(path), path)

  def getChildrenWatch(path: String): Future[Watched[Node.Children]] =
    printOp("getChildrenWatch", underlying.getChildrenWatch(path), path)

  def getData(path: String): Future[Node.Data] =
    printOp("getData", underlying.getData(path), path)

  def getDataWatch(path: String): Future[Watched[Node.Data]] =
    printOp("getDataWatch", underlying.getDataWatch(path), path)

  def sync(path: String): Future[Unit] =
    printOp("sync", underlying.sync(path), path)
}

private[serverset2] trait ChattyWriter extends ChattyClient with ZooKeeperWriter {
  protected val underlying: ZooKeeperWriter

  def create(
      path: String,
      data: Option[Buf],
      acl: Seq[Data.ACL],
      createMode: CreateMode): Future[String] =
    printOp(
      "create",
      underlying.create(path, data, acl, createMode),
      path,
      data.toString,
      acl.toString,
      createMode.toString)

  def delete(path: String, version: Option[Int]): Future[Unit] =
    printOp("delete", underlying.delete(path, version), path, version.toString)

  def setData(path: String, data: Option[Buf], version: Option[Int]): Future[Data.Stat] =
    printOp(
      "setData", underlying.setData(path, data, version), path, data.toString, version.toString)

  def setACL(path: String, acl: Seq[Data.ACL], version: Option[Int]): Future[Data.Stat] =
    printOp("setACL", underlying.setACL(path, acl, version), path, acl.toString, version.toString)
}

private[serverset2] trait ChattyMulti extends ChattyClient with ZooKeeperMulti {
  protected val underlying: ZooKeeperMulti

  def multi(ops: Seq[Op]): Future[Seq[OpResult]] =
    printOp("multi", underlying.multi(ops), ops.toString)
}

private[serverset2] trait ChattyRW extends ZooKeeperRW with ChattyReader with ChattyWriter {
  protected val underlying: ZooKeeperRW
}

private[serverset2] trait ChattyRWMulti extends ZooKeeperRWMulti with ChattyReader with ChattyWriter with ChattyMulti {
  protected val underlying: ZooKeeperRWMulti
}
