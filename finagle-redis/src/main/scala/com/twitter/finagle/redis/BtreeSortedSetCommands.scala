package com.twitter.finagle.redis

import java.lang.{Long => JLong}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.commands._
import com.twitter.io.Buf
import com.twitter.util.Future
import util.ReplyFormat

/**
 * These commands are specific to Twitter's internal fork of Redis
 * and will be removed eventually.
 */
private[redis] trait BtreeSortedSetCommands { self: BaseClient =>

  /**
   * Deletes `fields` from the given btree sorted set `key`.
   *
   * @return The number of fields deleted.
   */
  def bRem(key: Buf, fields: Seq[Buf]): Future[JLong] =
    doRequest(BRem(key, fields)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value for the `field` from the given btree sorted set `key`.
   *
   * @return `Some` if the field exists, `None` otherwise.
   */
  def bGet(key: Buf, field: Buf): Future[Option[Buf]] =
    doRequest(BGet(key, field)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  /**
   * Sets `field` : `value` pair in the given btree sorted set `key`.
   *
   * @return 1 if field is new, 0 if field was updated.
   */
  def bAdd(key: Buf, field: Buf, value: Buf): Future[JLong] =
    doRequest(BAdd(key, field, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns the btree sorted set cardinality for the given `key`.
   * Returns 0 if key does not exist.
   */
  def bCard(key: Buf): Future[JLong] =
    doRequest(BCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets all field value pairs for the given btree sorted order `key`
   * from `startField` to `endField`.
   */
  def bRange(
    key: Buf,
    count: Int,
    startField: Option[Buf],
    endField: Option[Buf]
  ): Future[Seq[(Buf, Buf)]] = {
    doRequest(BRange(key, Buf.Utf8(count.toString), startField, endField)) {
      case MBulkReply(messages) => Future.value(returnPairs(ReplyFormat.toBuf(messages)))
      case EmptyMBulkReply => Future.Nil
      case EmptyBulkReply => Future.Nil // on a cache miss
    }
  }

  def bMergeEx(key: Buf, fv: Map[Buf, Buf], milliseconds: Long): Future[Unit] = {
    doRequest(BMergeEx(key, fv, milliseconds)) {
      case StatusReply(msg) => Future.Unit
    }
  }
}
