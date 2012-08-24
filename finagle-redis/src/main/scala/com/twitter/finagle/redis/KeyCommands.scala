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
    doRequest(Del(keys.toList)) {
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
}