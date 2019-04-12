package com.twitter.finagle.redis

import java.lang.{Long => JLong, Boolean => JBoolean}
import com.twitter.finagle.redis.protocol.{IntegerReply, PFMerge, PFCount, PFAdd, StatusReply}
import com.twitter.io.Buf
import com.twitter.util.Future

private[redis] trait HyperLogLogCommands { self: BaseClient =>

  /**
   * Adds `elements` to a HyperLogLog data structure stored under hash `key`.
   *
   * @see https://redis.io/commands/pfadd
   *
   * @return Whether a bit was set in the HyperLogLog data structure.
   */
  def pfAdd(key: Buf, elements: List[Buf]): Future[JBoolean] =
    doRequest(PFAdd(key, elements)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Gets the approximated cardinality (number of unique elements) of sets
   * observed by the HyperLogLog at `keys`.
   *
   * @see https://redis.io/commands/pfcount
   */
  def pfCount(keys: Seq[Buf]): Future[JLong] =
    doRequest(PFCount(keys)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Merges HyperLogLogs at `srcKeys` to create a new HyperLogLog at `destKey`.
   *
   * @see https://redis.io/commands/pfmerge
   */
  def pfMerge(destKey: Buf, srcKeys: Seq[Buf]): Future[Unit] =
    doRequest(PFMerge(destKey, srcKeys)) {
      case StatusReply(_) => Future.Unit
    }
}
