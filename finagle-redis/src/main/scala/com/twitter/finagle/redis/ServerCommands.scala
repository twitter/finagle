package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol.StatusReply
import com.twitter.io.Buf
import com.twitter.util.Future
import com.twitter.finagle.redis.protocol._

private[redis] trait BasicServerCommands { self: BaseClient =>

  // TODO: CLIENT KILL

  // TODO: CLIENT LIST

  // TODO: CLIENT GETNAME

  // TODO: CLIENT PAUSE

  // TODO: CLIENT SETNAME

  /**
   * Returns information and statistics about the server
   * @return Buf with collection of \r\n terminated lines of the default info section
   */
  def info(): Future[Option[Buf]] =
    info(Buf.Empty)

  /**
   * Returns information and statistics about the server
   * @param section used to select a specific section of information.
   * @return a collection of \r\n terminated lines if server has info on the section
   */
  def info(section: Buf): Future[Option[Buf]] =
    doRequest(Info(section)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  // TODO: ROLE

  // TODO: SHUTDOWN
}

trait ServerCommands extends BasicServerCommands { self: BaseClient =>

  // TODO: BGREWRITEAOF

  // TODO: BGSAVE

  // TODO: COMMAND

  // TODO: COMMAND COUNT

  // TODO: COMMAND GETKEYS

  // TODO: COMMAND INFO

  // TODO: CONFIG GET

  // TODO: CONFIG REWRITE

  // TODO: CONFIG SET

  // TODO: CONFIG RESETSTAT

  def dbSize(): Future[Long] =
    doRequest(DBSize) {
      case IntegerReply(n) => Future.value(n)
    }

  // TODO: DEBUG OBJECT

  // TODO: DEBUG SEGFAULT

  /**
   * Deletes all keys in all databases
   */
  def flushAll(): Future[Unit] =
    doRequest(FlushAll) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Deletes all keys in current DB
   */
  def flushDB(): Future[Unit] =
    doRequest(FlushDB) {
      case StatusReply(message) => Future.Unit
    }

  // TODO: LAST SAVE

  // TODO: MONITOR

  def replicaOf(host: Buf, port: Buf): Future[Unit] =
    doRequest(ReplicaOf(host, port)) {
      case StatusReply(message) => Future.Unit
    }

  // TODO: SAVE

  def slaveOf(host: Buf, port: Buf): Future[Unit] =
    doRequest(SlaveOf(host, port)) {
      case StatusReply(message) => Future.Unit
    }

  // TODO: SLOWLOG

  // TODO: TIME
}
