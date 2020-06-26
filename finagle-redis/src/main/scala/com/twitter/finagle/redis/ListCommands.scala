package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf
import com.twitter.util.Future
import java.lang.{Long => JLong}
import com.twitter.finagle.redis.util.ReplyFormat

private[redis] trait ListCommands { self: BaseClient =>

  /**
   * Gets the length of the list stored at the hash `key`. If the key is a
   * non-list element, an exception will be thrown. Unassigned keys are
   * considered empty lists (has size 0).
   */
  def lLen(key: Buf): Future[JLong] =
    doRequest(LLen(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value of the element at the `index` position in the list stored
   * at the hash `key`. If the key is a non-list element, an exception will be
   * thrown.
   *
   * @return `Some` if the given element exists, `None` otherwise.
   */
  def lIndex(key: Buf, index: JLong): Future[Option[Buf]] =
    doRequest(LIndex(key, index)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  /**
   * Inserts a given `value` after another `pivot` value in the list stored
   * at the hash `key`. If the key is a non-list element, an exception will
   * be thrown.
   *
   * @return `Some` of the new length of the list. `None` if the pivot is not
   *        found, or the list is empty.
   */
  def lInsertAfter(key: Buf, pivot: Buf, value: Buf): Future[Option[JLong]] =
    doRequest(LInsert(key, "AFTER", pivot, value)) {
      case IntegerReply(n) => Future.value(if (n == -1) None else Some(n))
    }

  /**
   * Inserts a `value` before another `pivot` value in the list stored at the
   * hash `key`. If the key is a non-list element, an exception will be thrown.
   *
   * @return `Some` of the new length of the list, or `None` if the pivot is
   *        not found, or the list is empty.
   */
  def lInsertBefore(key: Buf, pivot: Buf, value: Buf): Future[Option[JLong]] =
    doRequest(LInsert(key, "BEFORE", pivot, value)) {
      case IntegerReply(n) => Future.value(if (n == -1) None else Some(n))
    }

  /**
   * Pops a value off the front of the list stored at the hash `key`. If the key
   * is a non-list element, an exception will be thrown.
   *
   * @return `Some` of the value of the popped element, or `None` if the list is
   *        empty.
   */
  def lPop(key: Buf): Future[Option[Buf]] =
    doRequest(LPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  /**
   * Pushes a list of `value` onto the front of the list stored at the hash
   * `key`. If the key is a non-list element, an exception will be thrown.
   *
   * @return The length of the list.
   */
  def lPush(key: Buf, values: List[Buf]): Future[JLong] =
    doRequest(LPush(key, values)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes `count` elements matching `value` from the list stored
   * at the hash `key`. If the key is a non-list element, an exception will
   * be thrown.
   *
   * @note The sign of `count` describes whether it will remove them from the
   *       back or the front of the list.  If `count` is 0, it will remove all
   *       instances.
   *
   * @return The number of removed elements.
   */
  def lRem(key: Buf, count: JLong, value: Buf): Future[JLong] =
    doRequest(LRem(key, count, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Creates a list at `key`, pushes given `values` onto the end of the list,
   * trims the list to `trim` length, and sets the ttl. `ttl` and `trim` are
   * not used if set to -1. If a list already exists at `key`, the list will
   * be overwritten. If the key is a non-list element, an exception will be thrown.
   *
   * This command is Twitter-specific. Most Redis implementations do not support
   * this method.
   */
  def lReset(key: Buf, values: List[Buf], ttl: JLong = -1, trim: JLong = -1): Future[Unit] =
    doRequest(LReset(key, values, ttl, trim)) {
      case StatusReply(message) => Future.Done
    }

  /**
   * Sets the element at `index` in the list stored under the hash `key` to a
   * given `value`. If the key is a non-list element, an exception will be thrown.
   */
  def lSet(key: Buf, index: JLong, value: Buf): Future[Unit] =
    doRequest(LSet(key, index, value)) {
      case StatusReply(message) => Future.Done
    }

  /**
   * Gets the values in the given range `start` - `end` (inclusive) of the list
   * stored at the hash `key`. If the key is a non-list element, an exception will
   * be thrown.
   */
  def lRange(key: Buf, start: JLong, end: JLong): Future[List[Buf]] =
    doRequest(LRange(key, start, end)) {
      case MBulkReply(message) => Future.value(ReplyFormat.toBuf(message))
      case EmptyMBulkReply => Future.value(List.empty)
    }

  /**
   * Pops a value off the end of the list stored at hash `key`. If the key is a
   * non-list element, an exception will be thrown.
   *
   * @return `Some` of the value of the popped element, or `None` if the list is
   *         empty.
   */
  def rPop(key: Buf): Future[Option[Buf]] =
    doRequest(RPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  /**
   * Pushes given `values` onto the end of the list stored at the hash `key`.
   * If the key is a non-list element, an exception will be thrown.
   *
   * @return The length of the list.
   */
  def rPush(key: Buf, values: List[Buf]): Future[JLong] =
    doRequest(RPush(key, values)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes all of the elements from the list stored at hash `key`, except for
   * those in the range: `start` - `end` (inclusive).
   */
  def lTrim(key: Buf, start: JLong, end: JLong): Future[Unit] =
    doRequest(LTrim(key, start, end)) {
      case StatusReply(message) => Future.Done
    }

  /**
   * Atomically returns and removes the last element (tail) of the list stored at source,
   * and pushes the element at the first element (head) of the list stored at destination
   *
   * @return `Some` of the value of the popped element, or `None` if the list is
   *         empty.
   */
  def rPopLPush(source: Buf, dest: Buf): Future[Option[Buf]] =
    doRequest(RPopLPush(source, dest)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }
}
