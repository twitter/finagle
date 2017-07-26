package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol.{BulkReply, EmptyBulkReply, StatusReply}
import com.twitter.finagle.redis.protocol.{TopologyAdd, TopologyDelete, TopologyGet}
import com.twitter.io.Buf
import com.twitter.util.Future

/**
 * @note These commands are specific to Twitter's internal fork of Redis
 * and will be removed eventually.
 */
private[redis] trait TopologyCommands { self: BaseClient =>

  /**
   * Adds a `key` : `value` pair to a topology.
   */
  def topologyAdd(key: Buf, value: Buf): Future[Unit] =
    doRequest(TopologyAdd(key, value)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Gets a value stored under `key` from a topology.
   */
  def topologyGet(key: Buf): Future[Option[Buf]] =
    doRequest(TopologyGet(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  /**
   * Deletes a pair with a `key` from a topology.
   */
  def topologyDelete(key: Buf): Future[Unit] =
    doRequest(TopologyDelete(key)) {
      case StatusReply(message) => Future.Unit
    }
}
