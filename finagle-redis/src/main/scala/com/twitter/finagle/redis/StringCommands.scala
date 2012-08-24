package com.twitter.finagle.redis

import _root_.java.lang.{Long => JLong}
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BytesToString, NumberFormat, ReplyFormat}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.Future
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}


trait Strings { self: BaseClient =>

  /**
   * Appends value at the given key. If key doesn't exist,
   * behavior is similar to SET command
   * @params key, value
   * @return length of string after append operation
   */
  def append(key: ChannelBuffer, value: ChannelBuffer): Future[JLong] =
    doRequest(Append(key, value)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Decrements number stored at key by given amount. If key doesn't
   * exist, value is set to 0 before the operation
   * @params key, amount
   * @return value after decrement. Error if key contains value
   * of the wrong type
   */
  def decrBy(key: ChannelBuffer, amount: Long): Future[JLong] =
    doRequest(DecrBy(key, amount)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the value associated with the given key
   * @param key
   * @return value, or none if key doesn't exist
   */
  def get(key: ChannelBuffer): Future[Option[ChannelBuffer]] =
    doRequest(Get(key)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Gets the substring of the value associated with given key
   * @params key, start, end
   * @return substring, or none if key doesn't exist
   */
  def getRange(key: ChannelBuffer, start: Long, end: Long): Future[Option[ChannelBuffer]] =
    doRequest(GetRange(key, start, end)) {
      case BulkReply(message)   => Future.value(Some(message))
      case EmptyBulkReply()     => Future.value(None)
    }

  /**
   * Sets the given value to key. If a value already exists for the key,
   * the value is overwritten with the new value
   * @params key, value
   */
  def set(key: ChannelBuffer, value: ChannelBuffer): Future[Unit] =
    doRequest(Set(key, value)) {
      case StatusReply(message) => Future.Unit
    }
}