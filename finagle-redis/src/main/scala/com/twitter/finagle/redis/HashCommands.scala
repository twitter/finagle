package com.twitter.finagle.redis

import _root_.java.lang.{Long => JLong}
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BytesToString, NumberFormat, ReplyFormat}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.Future
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}


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
      case EmptyMBulkReply()    => Future.value(Seq())
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
      case EmptyMBulkReply()    => Future.value(Seq())
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
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Returns keys in given hash, starting at cursor
   * @param hash key, cursor, count, pattern
   * @return cursor followed by matching keys
   */
  def hScan(key: ChannelBuffer, cursor: Long, count: Option[Long], pattern: Option[ChannelBuffer]
  ): Future[Seq[ChannelBuffer]] =
    doRequest(HScan(key, cursor, count, pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.value(Seq())
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
}