package com.twitter.finagle.redis

import _root_.java.lang.{Long => JLong}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.commands._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer
import util.ReplyFormat

trait BtreeSortedSetCommands { self: BaseClient =>

  /**
   * Deletes fields from the given btree sorted set key
   * @param key, fields
   * @return Number of fields deleted
   */
  def bRem(key: ChannelBuffer, fields: Seq[ChannelBuffer]): Future[JLong] =
    doRequest(BRem(key, fields)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value for the field from the given btree sorted set key
   * @param key, field
   * @return Value if field exists
   */
  def bGet(key: ChannelBuffer, field: ChannelBuffer): Future[Option[ChannelBuffer]] =
    doRequest(BGet(key, field)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Sets field value pair in the given btree sorted set key
   * @param key, field, value
   * @return 1 if field is new, 0 if field was updated
   */
  def bAdd(key: ChannelBuffer, field: ChannelBuffer, value: ChannelBuffer): Future[JLong] =
    doRequest(BAdd(key, field, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns the btree sorted set cardinality for the given key
   * @param key
   * @return Integer representing cardinality of btree sorted set,
   * or 0 if key does not exist
   */
  def bCard(key: ChannelBuffer): Future[JLong] =
    doRequest(BCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }
  /**
   * Gets all field value pairs for the given btree sorted order key from startField to endField
   * @param key
   * @return Sequence of field/value pairs
   */
  def bRange(key: ChannelBuffer, startField: Option[ChannelBuffer], endField: Option[ChannelBuffer]):
    Future[Seq[(ChannelBuffer, ChannelBuffer)]] = {
    doRequest(BRange(key, startField, endField)) {
      case MBulkReply(messages) => Future.value(
        returnPairs(ReplyFormat.toChannelBuffers(messages)))
      case EmptyMBulkReply()    => Future.Nil
    }
  }
}
