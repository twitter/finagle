package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol.StatusReply
import com.twitter.util.Future
import com.twitter.finagle.redis.protocol._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

trait BasicServerCommands { self: BaseClient =>

  // TODO: CLIENT KILL

  // TODO: CLIENT LIST

  // TODO: CLIENT GETNAME

  // TODO: CLIENT PAUSE

  // TODO: CLIENT SETNAME

  /**
   * Returns information and statistics about the server
   * @param section Optional parameter can be used to select a specific section of information
   * @return ChannelBuffer with collection of \r\n terminated lines if server has info on section
   */
  def info(section: ChannelBuffer = ChannelBuffers.EMPTY_BUFFER): Future[Option[ChannelBuffer]] =
    doRequest(Info(section)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply()   => Future.value(None)
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

  // TODO: DBSIZE

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

  // TODO: SAVE

  def slaveOf(host: ChannelBuffer, port: ChannelBuffer): Future[Unit] =
    doRequest(SlaveOf(host, port)) {
      case StatusReply(message) => Future.Unit
    }

  // TODO: SLOWLOG

  // TODO: TIME
}
