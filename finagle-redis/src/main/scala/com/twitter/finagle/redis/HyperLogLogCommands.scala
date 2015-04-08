package com.twitter.finagle.redis

import _root_.java.lang.{Long => JLong,Boolean => JBoolean}
import com.twitter.finagle.redis.protocol.{IntegerReply, PFMerge, PFCount, PFAdd, StatusReply}
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

trait HyperLogLogs { self: BaseClient =>

  /**
   * Adds elements to a HyperLogLog data structure.
   *
   * @param key
   * @param elements
   * @return True if a bit was set in the HyperLogLog data structure
   * @see http://redis.io/commands/pfadd
   */
  def pfAdd(key: ChannelBuffer, elements: List[ChannelBuffer]): Future[JBoolean] =
    doRequest(PFAdd(key, elements)) {
      case IntegerReply(n) => Future.value(n == 1)
    }


  /**
   * Get the approximated cardinality of sets observed by the HyperLogLog at key(s)
   * @param keys
   * @return Approximated number of unique elements
   * @see http://redis.io/commands/pfcount
   */
  def pfCount(keys: Seq[ChannelBuffer]): Future[JLong] =
    doRequest(PFCount(keys)) {
      case IntegerReply(n) => Future.value(n)
    }


  /**
   * Merge HyperLogLogs at srcKeys to create a new HyperLogLog at destKey
   * @param destKey
   * @param srcKeys
   * @see http://redis.io/commands/pfmerge
   */
  def pfMerge(destKey: ChannelBuffer, srcKeys: Seq[ChannelBuffer]): Future[Unit] =
    doRequest(PFMerge(destKey, srcKeys)) {
      case StatusReply(_) => Future.Unit
    }

}
