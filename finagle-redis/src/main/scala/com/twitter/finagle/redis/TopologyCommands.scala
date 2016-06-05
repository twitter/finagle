package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol.{BulkReply, EmptyBulkReply, StatusReply}
import com.twitter.finagle.redis.protocol.{TopologyAdd, TopologyDelete, TopologyGet}
import com.twitter.io.Buf
import com.twitter.util.Future

/**
 * These commands are specific to twitter's internal fork of redis
 * and will be removed eventually
 */
trait TopologyCommands { self: BaseClient =>

  /**
   * Adds a key to topology
   */
  def topologyAdd(key: Buf, value: Buf): Future[Unit] =
    doRequest(TopologyAdd(key, value)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Gets a key from topology
   */
  def topologyGet(key: Buf): Future[Option[Buf]] =
    doRequest(TopologyGet(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply()   => Future.None
    }

  /**
   * Deletes a key from topology
   */
  def topologyDelete(key: Buf): Future[Unit] =
    doRequest(TopologyDelete(key)) {
      case StatusReply(message) => Future.Unit
    }
}
