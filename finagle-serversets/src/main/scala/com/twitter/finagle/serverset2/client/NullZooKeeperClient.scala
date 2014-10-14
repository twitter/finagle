package com.twitter.finagle.serverset2.client

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client.Data.Stat
import com.twitter.finagle.serverset2.client.Node.{ACL, Children}
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future, Time}

/**
 * Provide a Null client, useful as an initial value for a wrapper that
 * re-establishes a ZooKeeper connection on SessionExpiration, as
 * ZooKeeperClient is designed for single session operation.
 */
trait NullZooKeeperClient extends ZooKeeperClient {
  def sessionId: Long = -1
  def getEphemerals(): Future[Seq[String]] = Future.never
  def addAuthInfo(scheme: String, auth: Buf): Future[Unit] = Future.never
  def sessionTimeout: Duration = 0.seconds
  def sessionPasswd: Buf = Buf.Empty
  def close(deadline: Time): Future[Unit] = Future.never
}

trait NullZooKeeperReader extends ZooKeeperReader with NullZooKeeperClient {
  def exists(path: String): Future[Option[Stat]] = Future.never
  def getDataWatch(path: String): Future[Watched[Node.Data]] = Future.never
  def sync(path: String): Future[Unit] = Future.never
  def getData(path: String): Future[Node.Data] = Future.never
  def existsWatch(path: String): Future[Watched[Option[Stat]]] = Future.never
  def getACL(path: String): Future[ACL] = Future.never
  def getChildrenWatch(path: String): Future[Watched[Children]] = Future.never
  def getChildren(path: String): Future[Children] = Future.never
}

trait NullZooKeeperWriter extends ZooKeeperWriter with NullZooKeeperClient {
  def create(
    path: String,
    data: Option[Buf],
    acl: Seq[Data.ACL],
    createMode: CreateMode
  ): Future[String] = Future.never

  def delete(path: String, version: Option[Int]): Future[Unit] = Future.never
  def setData(path: String, data: Option[Buf], version: Option[Int]): Future[Stat] = Future.never
  def setACL(path: String, acl: Seq[Data.ACL], version: Option[Int]): Future[Stat] = Future.never
}

trait NullZooKeeperMulti extends ZooKeeperMulti with NullZooKeeperClient {
  def multi(ops: Seq[Op]): Future[Seq[OpResult]] = Future.never
}

trait NullZooKeeperRW extends ZooKeeperRW with NullZooKeeperReader with NullZooKeeperWriter
trait NullZooKeeperRWMulti extends ZooKeeperRWMulti with NullZooKeeperReader with NullZooKeeperWriter with NullZooKeeperMulti

object NullZooKeeperClient extends NullZooKeeperClient
object NullZooKeeperReader extends NullZooKeeperReader
object NullZooKeeperWriter extends NullZooKeeperWriter
object NullZooKeeperMulti extends NullZooKeeperMulti

object NullZooKeeperRW extends NullZooKeeperRW
object NullZooKeeperRWMulti extends NullZooKeeperRWMulti
