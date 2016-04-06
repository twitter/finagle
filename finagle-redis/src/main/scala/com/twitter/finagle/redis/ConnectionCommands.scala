package com.twitter.finagle.redis

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

trait BasicConnectionCommands { self: BaseClient =>

  /**
    * Sends a PING to Redis instance
    */
  def ping(): Future[Unit] =
    doRequest(Ping) {
      case StatusReply("PONG") => Future.Unit
    }

  /**
   * Closes connection to Redis instance
   */
  def quit(): Future[Unit] =
    doRequest(Quit) {
      case StatusReply(message) => Future.Unit
    }
}

trait ConnectionCommands extends BasicConnectionCommands { self: BaseClient =>

  /**
   * Authorizes to db
   * @param password
   */
  @deprecated("remove netty3 types from public API", "2016-03-15")
  def auth(password: ChannelBuffer): Future[Unit] =
    auth(ChannelBufferBuf.Owned(password))

  /**
   * Authorizes to db
   * @param password
   */
  def auth(password: Buf): Future[Unit] =
    doRequest(Auth(password)) {
      case StatusReply(message) => Future.Unit
    }

  // TODO: ECHO

  // TODO: PING with argument

  /**
   * Select DB with specified zero-based index
   * @param index
   */
  def select(index: Int): Future[Unit] =
    doRequest(Select(index)) {
      case StatusReply(message) => Future.Unit
    }
}
