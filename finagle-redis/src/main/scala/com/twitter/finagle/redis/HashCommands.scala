package com.twitter.finagle.redis

import java.lang.{Boolean => JBoolean, Long => JLong}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.io.Buf
import com.twitter.util.Future

private[redis] trait HashCommands { self: BaseClient =>

  /**
   * Deletes `fields` from given hash `key`. Returns the number of fields deleted.
   */
  def hDel(key: Buf, fields: Seq[Buf]): Future[JLong] =
    doRequest(HDel(key, fields)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Determines if a hash `field` exists on a given hash `key`. Returns boolean
   * signaling whether the field exists.
   */
  def hExists(key: Buf, field: Buf): Future[JBoolean] =
    doRequest(HExists(key, field)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Gets `field` from a given hash `key`.
   */
  def hGet(key: Buf, field: Buf): Future[Option[Buf]] =
    doRequest(HGet(key, field)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  /**
   * Gets all field value pairs for given hash `key`.
   */
  def hGetAll(key: Buf): Future[Seq[(Buf, Buf)]] =
    doRequest(HGetAll(key)) {
      case MBulkReply(messages) => Future.value(returnPairs(ReplyFormat.toBuf(messages)))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Increments a `field` on a given hash `key` by `amount`. Returns new field value.
   */
  def hIncrBy(key: Buf, field: Buf, amount: Long): Future[JLong] =
    doRequest(HIncrBy(key, field, amount)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns all field names stored at the hash `key`.
   *
   */
  def hKeys(key: Buf): Future[Seq[Buf]] =
    doRequest(HKeys(key)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Returns the number of fields stored at the hash `key`.
   */
  def hLen(key: Buf): Future[Long] =
    doRequest(HLen(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets values for given fields stored at the hash `key`.
   */
  def hMGet(key: Buf, fields: Seq[Buf]): Future[Seq[Buf]] =
    doRequest(HMGet(key, fields)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Sets values for given fields stored at the hash `key`.
   */
  def hMSet(key: Buf, fv: Map[Buf, Buf]): Future[Unit] =
    doRequest(HMSet(key, fv)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Sets values for given fields stored at the hash `key` and sets the ttl.
   */
  def hMSetEx(key: Buf, fv: Map[Buf, Buf], milliseconds: Long): Future[Unit] =
    doRequest(HMSetEx(key, fv, milliseconds)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Adds values for given fields stored at the hash `key` if it doesn't exist
   * and sets the ttl. Version set at the destination is retained if it already exists.
   */
  def hMergeEx(key: Buf, fv: Map[Buf, Buf], milliseconds: Long): Future[Unit] =
    doRequest(HMergeEx(key, fv, milliseconds)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Returns keys in given hash `key`, starting at `cursor`.
   */
  def hScan(key: Buf, cursor: JLong, count: Option[JLong], pattern: Option[Buf]): Future[Seq[Buf]] =
    doRequest(HScan(key, cursor, count, pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Sets `field` stored at given hash `key` to a given `value`.
   * Returns `1` if fields is new, `0` if field was updated.
   */
  def hSet(key: Buf, field: Buf, value: Buf): Future[JLong] =
    doRequest(HSet(key, field, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Sets `field` stored at given hash `key` to a given `value` only if the field
   * Returns `1` if fields is new, `0` no operation was performed.
   */
  def hSetNx(key: Buf, field: Buf, value: Buf): Future[JLong] =
    doRequest(HSetNx(key, field, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the values of all fields in given hash `key`. Returns empty
   * list if key does not exist.
   */
  def hVals(key: Buf): Future[Seq[Buf]] =
    doRequest(HVals(key)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

  def hStrlen(key: Buf, field: Buf): Future[Long] =
    doRequest(HStrlen(key, field)) {
      case IntegerReply(n) => Future.value(n)
    }
}
