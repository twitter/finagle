package com.twitter.finagle.redis

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.{Future, Time}
import org.jboss.netty.buffer.ChannelBuffer


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
   * Sets how long it will take the key to expire
   * @params key, ttl
   * @return boolean, true if it successfully set the ttl (time to live) on a valid key,
   * false otherwise.
   */
  def expire(key: ChannelBuffer, ttl: JLong): Future[JBoolean] =
    doRequest(Expire(key, ttl)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Same effect and semantic as "expire", but takes an absolute Unix timestamp
   * @param key, ttl (unix timestamp)
   * @return boolean, true if it successfully set the ttl (time to live) on a valid key,
   * false otherwise.
   */
  def expireAt(key: ChannelBuffer, ttl: JLong): Future[JBoolean] =
    doRequest(ExpireAt(key, Time.fromMilliseconds(ttl))) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Returns all keys matching pattern
   * @param pattern
   * @return list of keys matching pattern
   */
  def keys(pattern: ChannelBuffer): Future[Seq[ChannelBuffer]] =
    doRequest(Keys(pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.Nil
    }

  /**
   * Set a key's time to live in milliseconds.
   *
   * @param key, milliseconds
   * @return true if the timeout was set.
   *         false if key does not exist or the timeout could not be set.
   * @see http://redis.io/commands/pexpire
   */
  def pExpire(key: ChannelBuffer, milliseconds: JLong): Future[JBoolean] =
    doRequest(PExpire(key, milliseconds)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Set the expiration for a key as a UNIX timestamp specified in milliseconds.
   *
   * @param key, timestamp
   * @return true if the timeout was set.
   *         false if key does not exist or the timeout could not be set
   *         (see: EXPIRE).
   * @see http://redis.io/commands/pexpireat
   */
  def pExpireAt(key: ChannelBuffer, timestamp: JLong): Future[JBoolean] =
    doRequest(PExpireAt(key, Time.fromMilliseconds(timestamp))) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Get the time to live for a key in milliseconds.
   *
   * @param key
   * @return Time to live in milliseconds or None when key does not exist or
   *         does not have a timeout.
   * @see
   */
  def pTtl(key: ChannelBuffer): Future[Option[JLong]] =
    doRequest(PTtl(key)) {
      case IntegerReply(n) =>
        if (n != -1) Future.value(Some(n))
        else Future.value(None)
    }

  /**
   * Returns keys starting at cursor
   * @param cursor, count, pattern
   * @return cursor followed by matching keys
   */
  def scan(cursor: JLong, count: Option[JLong], pattern: Option[ChannelBuffer]
  ): Future[Seq[ChannelBuffer]] =
    doRequest(Scan(cursor, count, pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.Nil
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

}
