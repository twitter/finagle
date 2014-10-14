package com.twitter.finagle.redis

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer


trait Hashes { self: BaseClient =>

  /**
   * Deletes fields from given hash
   * @param hash key, fields
   * @return Number of fields deleted
   */
  def hDel(key: ChannelBuffer, fields: Seq[ChannelBuffer]): Future[JLong] =
    doRequest(HDel(key, fields)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Determine if a hash field exists
   * @param hash key, field
   * @return true if key field exists, false otherwise
   */
  def hExists(key: ChannelBuffer, field: ChannelBuffer): Future[JBoolean] =
    doRequest(HExists(key, field)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Gets field from hash
   * @param hash key, field
   * @return Value if field exists
   */
  def hGet(key: ChannelBuffer, field: ChannelBuffer): Future[Option[ChannelBuffer]] =
    doRequest(HGet(key, field)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Gets all field value pairs for given hash
   * @param hash key
   * @return Sequence of field/value pairs
   */
  def hGetAll(key: ChannelBuffer): Future[Seq[(ChannelBuffer, ChannelBuffer)]] =
    doRequest(HGetAll(key)) {
      case MBulkReply(messages) => Future.value(
        returnPairs(ReplyFormat.toChannelBuffers(messages)))
      case EmptyMBulkReply()    => Future.Nil
  }

  /**
   * Increment a field by a value
   * @param hash key, fields, amount
   * @return new value of field
   */
  def hIncrBy(key: ChannelBuffer, field: ChannelBuffer, amount: Long): Future[JLong] =
    doRequest(HIncrBy(key, field, amount)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Return all field names stored at key
   * @param hash key
   * @return List of fields in hash
   */
  def hKeys(key: ChannelBuffer): Future[Seq[ChannelBuffer]] =
    doRequest(HKeys(key)) {
      case MBulkReply(messages) => Future.value(
        ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.Nil
    }

  /**
   * Gets values for given fields in hash
   * @param hash key, fields
   * @return List of values
   */
  def hMGet(key: ChannelBuffer, fields: Seq[ChannelBuffer]): Future[Seq[ChannelBuffer]] =
    doRequest(HMGet(key, fields)) {
      case MBulkReply(messages) => Future.value(
        ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.Nil
    }

  /**
   * Sets values for given fields in hash
   * @param key hash key
   * @param fv map of field to value
   * @see http://redis.io/commands/hmset
   */
  def hMSet(key: ChannelBuffer, fv: Map[ChannelBuffer, ChannelBuffer]): Future[Unit] =
    doRequest(HMSet(key, fv)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Returns keys in given hash, starting at cursor
   * @param hash key, cursor, count, pattern
   * @return cursor followed by matching keys
   */
  def hScan(key: ChannelBuffer, cursor: JLong, count: Option[JLong], pattern: Option[ChannelBuffer]
  ): Future[Seq[ChannelBuffer]] =
    doRequest(HScan(key, cursor, count, pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.Nil
    }

  /**
   * Sets field value pair in given hash
   * @param hash key, field, value
   * @return 1 if field is new, 0 if field was updated
   */
  def hSet(key: ChannelBuffer, field: ChannelBuffer, value: ChannelBuffer): Future[JLong] =
    doRequest(HSet(key, field, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Sets field value pair in given hash only if the field does not yet exist
   * @param hash key, field, value
   * @return 1 if field is new, 0 if no operation was performed
   */
  def hSetNx(key: ChannelBuffer, field: ChannelBuffer, value: ChannelBuffer): Future[JLong] =
    doRequest(HSetNx(key, field, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the values of all fields in given hash
   * @param hash key
   * @return list of values, or empty list when key does not exist
   */
  def hVals(key: ChannelBuffer): Future[Seq[ChannelBuffer]] =
    doRequest(HVals(key)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.Nil
    }
}
