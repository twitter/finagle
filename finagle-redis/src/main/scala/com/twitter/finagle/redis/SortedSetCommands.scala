package com.twitter.finagle.redis

import _root_.java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BytesToString, NumberFormat, ReplyFormat}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.Future
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}


trait SortedSets { self: BaseClient =>

  /**
   * Adds member, score pair to sorted set
   * @params key, score, member
   * @return Number of elements added to sorted set
   */
  def zAdd(key: ChannelBuffer, score: Double, member: ChannelBuffer): Future[JLong] =
    doRequest(ZAdd(key, List(ZMember(score, member)))) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns sorted set cardinality of the sorted set at key
   * @param key
   * @return Integer representing cardinality of sorted set,
   * or 0 if key does not exist
   */
  def zCard(key: ChannelBuffer): Future[JLong] =
    doRequest(ZCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets number of elements in sorted set with score between min and max
   * @params key, min, max
   * @return Number of elements between min and max in sorted set
   */
  def zCount(key: ChannelBuffer, min: ZInterval, max: ZInterval): Future[JLong] =
    doRequest(ZCount(key, min, max)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets member, score pairs from sorted set between min and max
   * Results are limited by offset and count
   * @param key, min, max, withscores flag, limit
   * @return ZRangeResults object containing item/score pairs
   */
  def zRangeByScore(
    key: ChannelBuffer,
    min: ZInterval,
    max: ZInterval,
    withScores: Boolean,
    limit: Option[Limit]
  ): Future[ZRangeResults] =
    doRequest(
      ZRangeByScore(key, min, max, (if (withScores) WithScores.asArg else None), limit)
    ) {
      case MBulkReply(messages) => Future.value(
        ZRangeResults(returnPairs(ReplyFormat.toChannelBuffers(messages))))
      case EmptyMBulkReply()    => Future.value(ZRangeResults(List()))
    }

  /**
   * Removes specified member(s) from sorted set at key
   * @params key, member(s)
   * @return Number of members removed from sorted set
   */
  def zRem(key: ChannelBuffer, members: Seq[ChannelBuffer]): Future[JLong] =
    doRequest(ZRem(key, members.toList)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns specified range of elements in sorted set at key
   * Elements are ordered from highest to lowest score
   * @param key, start, stop
   * @return List of elements in specified range
   */
  def zRevRange(key: ChannelBuffer, start: Long, stop: Long): Future[Seq[ChannelBuffer]] =
    doRequest(ZRevRange(key, start, stop)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply()    => Future.value(Seq())
    }

  /**
   * Returns elements in sorted set at key with a score between max and min
   * Elements are ordered from highest to lowest score
   * Results are limited by offset and count
   * @param key, max, min, withscores flag, limit
   * @return ZRangeResults object containing item/score pairs
   */
  def zRevRangeByScore(
    key: ChannelBuffer,
    max: ZInterval,
    min: ZInterval,
    withScores: Boolean,
    limit: Option[Limit]
  ): Future[ZRangeResults] =
    doRequest(
      ZRevRangeByScore(key, max, min, (if (withScores) WithScores.asArg else None), limit)
    ) {
      case MBulkReply(messages) => Future.value(
        ZRangeResults(returnPairs(ReplyFormat.toChannelBuffers(messages))))
      case EmptyMBulkReply()    => Future.value(ZRangeResults(List()))
    }

  /**
   * Gets score of member in sorted set
   * @param key, member
   * @return Score of member
   */
  def zScore(key: ChannelBuffer, member: ChannelBuffer): Future[Option[JDouble]] =
    doRequest(ZScore(key, member)) {
      case BulkReply(message)   =>
        Future.value(Some(NumberFormat.toDouble(BytesToString(message.array))))
      case EmptyBulkReply()     => Future.value(None)
    }
}