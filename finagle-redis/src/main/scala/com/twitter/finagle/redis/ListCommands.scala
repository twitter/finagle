package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer
import _root_.java.lang.{Long => JLong}
import com.twitter.finagle.redis.util.ReplyFormat

trait Lists { self: BaseClient =>
  /**
   * Gets the length of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @return the length of the list.  Unassigned keys are considered empty
   * lists, and return 0.
   */
  def lLen(key: ChannelBuffer): Future[JLong] =
    doRequest(LLen(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value of the element at the indexth position in the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @param index
   * @return an option of the value of the element at the indexth position in the list.
   * Nothing if the index is out of range.
   */
  def lIndex(key: ChannelBuffer, index: JLong): Future[Option[ChannelBuffer]] =
    doRequest(LIndex(key, index)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply()   => Future.value(None)
    }

  /**
   * Inserts a value after another pivot value in the list.
   * If the key is a non-list element,
   * an exception will be thrown.
   * @param key
   * @param pivot
   * @param value
   * @return an option of the new length of the list, or nothing if the pivot is not found, or
   * the list is empty.
   */
  def lInsertAfter(
    key: ChannelBuffer,
    pivot: ChannelBuffer,
    value: ChannelBuffer
  ): Future[Option[JLong]] =
    doRequest(LInsert(key, "AFTER", pivot, value)) {
      case IntegerReply(n) => Future.value(if (n == -1) None else Some(n))
    }

  /**
   * Inserts a value before another pivot value in the list.
   * If the key is a non-list element,
   * an exception will be thrown.
   * @param key
   * @param pivot
   * @param value
   * @return an option of the new length of the list, or nothing if the pivot is not found, or the
   * list is empty.
   */
  def lInsertBefore(
    key: ChannelBuffer,
    pivot: ChannelBuffer,
    value: ChannelBuffer
  ): Future[Option[JLong]] =
    doRequest(LInsert(key, "BEFORE", pivot, value)) {
      case IntegerReply(n) => Future.value(if (n == -1) None else Some(n))
    }

  /**
   * Pops a value off the front of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @return an option of the value of the popped element, or nothing if the list is empty.
   */
  def lPop(key: ChannelBuffer): Future[Option[ChannelBuffer]] =
    doRequest(LPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply() => Future.value(None)
    }

  /**
   * Pushes a value onto the front of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @param value
   * @return the length of the list
   */
  def lPush(key: ChannelBuffer, value: List[ChannelBuffer]): Future[JLong] =
    doRequest(LPush(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes count elements matching value from the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @param count
   * @note The sign of `count` describes whether it will remove them from the
   * back or the front of the list.  If count is 0, it will remove all instances, value
   * @return the number of removed elements.
   */
  def lRem(key: ChannelBuffer, count: JLong, value: ChannelBuffer): Future[JLong] =
    doRequest(LRem(key, count, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Sets the indexth element to be value.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @param index
   * @param value
   */
  def lSet(key: ChannelBuffer, index: JLong, value: ChannelBuffer): Future[Unit] =
    doRequest(LSet(key, index, value)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Gets the values in the range supplied.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @param start (inclusive)
   * @param end (inclusive)
   * @return a list of the value
   */
  def lRange(key: ChannelBuffer, start: JLong, end: JLong): Future[List[ChannelBuffer]] =
    doRequest(LRange(key, start, end)) {
      case MBulkReply(message) => Future.value(ReplyFormat.toChannelBuffers(message))
      case EmptyMBulkReply() => Future.value(List())
    }

  /**
   * Pops a value off the end of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @return an option of the value of the popped element, or nothing if the list is empty.
   */
  def rPop(key: ChannelBuffer): Future[Option[ChannelBuffer]] =
    doRequest(RPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply() => Future.value(None)
    }

  /**
   * Pushes a value onto the end of the list.
   * If the key is a non-list element, an exception will be thrown.
   * @param key
   * @param value
   * @return the length of the list
   */
  def rPush(key: ChannelBuffer, value: List[ChannelBuffer]): Future[JLong] =
    doRequest(RPush(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes all of the elements from the list except for those in the range.
   * @param key
   * @param start (inclusive)
   * @param end (exclusive)
   */
  def lTrim(key: ChannelBuffer, start: JLong, end: JLong): Future[Unit] =
    doRequest(LTrim(key, start, end)) {
      case StatusReply(message) => Future.Unit
    }
}
