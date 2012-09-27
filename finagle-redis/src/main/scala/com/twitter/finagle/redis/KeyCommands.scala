package com.twitter.finagle.redis

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BytesToString, NumberFormat, ReplyFormat}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.Future
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}


trait Keys { self: BaseClient =>

  /**
   * Removes keys
   * @param list of keys to remove
   * @return Number of keys removed
   */
  def del(keys: Seq[ChannelBuffer]): Future[JLong] =
    doRequest(Del(keys)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Checks if given key exists
   * @param key
   * @return true if key exists, false otherwise
   */
  def exists(key: ChannelBuffer): Future[JBoolean] =
    doRequest(Exists(key)) {
      case IntegerReply(n) => Future.value((n == 1))
    }

  /**
   * Returns all keys matching pattern
   * @param pattern
   * @return list of keys matching pattern
   */
  def keys(pattern: ChannelBuffer): Future[Seq[ChannelBuffer]] =
    doRequest(Keys(pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Returns keys starting at cursor
   * @param cursor, count, pattern
   * @return cursor followed by matching keys
   */
  def scan(cursor: Long, count: Option[Long], pattern: Option[ChannelBuffer]
  ): Future[Seq[ChannelBuffer]] =
    doRequest(Scan(cursor, count, pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Gets the ttl of the given key.
   * @param key
   * @return Option containing either the ttl in seconds if the key exists 
   * and has a timeout, or else nothing.
   */
  def ttl(key: ChannelBuffer): Future[Option[JLong]] =
    doRequest(Ttl(key)) {
      case IntegerReply(n) => {
        if (n != -1) {
          Future.value(Some(n))
        }
        else {
          Future.value(None)
        }
      }
    }

  /**
   * Sets how long it will take the key to expire
   * @params key, ttl
   * @return boolean, true if it successfully set the ttl (time to live) on a valid key,
   * false otherwise.
   */
  def expire(key: ChannelBuffer, ttl: JLong): Future[JBoolean] =
    doRequest(Expire(key, ttl)) {
      case IntegerReply(n) => Future.value(n == 1)
    }
}
