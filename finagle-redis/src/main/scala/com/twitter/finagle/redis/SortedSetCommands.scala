package com.twitter.finagle.redis

import _root_.java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.BytesToString
import com.twitter.finagle.redis.util.NumberFormat
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer


trait SortedSets { self: BaseClient =>
  private[this] def parseMBulkReply(
    withScores: JBoolean
  ): PartialFunction[Reply, Future[Either[ZRangeResults, Seq[ChannelBuffer]]]] = {
    val parse: PartialFunction[Reply, Either[ZRangeResults, Seq[ChannelBuffer]]] = {
      case MBulkReply(messages) => withScoresHelper(withScores)(messages)
      case EmptyMBulkReply() => withScoresHelper(withScores)(Nil)
    }
    parse andThen Future.value
  }

  private[this] def withScoresHelper(
    withScores: JBoolean
  )(messages: List[Reply]): Either[ZRangeResults, Seq[ChannelBuffer]] = {
    val chanBufs = ReplyFormat.toChannelBuffers(messages)
    if (withScores)
      Left(ZRangeResults(returnPairs(chanBufs)))
    else
      Right(chanBufs)
  }

  /**
   * Add a member with score to a sorted set
   * @param key
   * @param score
   * @param member
   * @return Number of elements added to sorted set
   */
  def zAdd(key: ChannelBuffer, score: JDouble, member: ChannelBuffer): Future[JLong] = {
    zAddMulti(key, Seq((score, member)))
  }

  /**
   * Adds member, score pairs to sorted set
   * @param key
   * @param members sequence of (score, member) tuples
   * @return Number of elements added to sorted set
   * @note Adding multiple elements only works with redis 2.4 or later.
   */
  def zAddMulti(key: ChannelBuffer, members: Seq[(JDouble, ChannelBuffer)]): Future[JLong] = {
    doRequest(ZAdd(key, members.map { m => ZMember(m._1, m._2) })) {
      case IntegerReply(n) => Future.value(n)
    }
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
   * @param key
   * @param min
   * @param max
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
    withScores: JBoolean,
    limit: Option[Limit]
  ): Future[Either[ZRangeResults, Seq[ChannelBuffer]]] =
    doRequest(
      ZRangeByScore(key, min, max, WithScores.option(withScores), limit)
    ) (parseMBulkReply(withScores))

  /**
   * Removes specified member(s) from sorted set at key
   * @param key
   * @param members
   * @return Number of members removed from sorted set
   */
  def zRem(key: ChannelBuffer, members: Seq[ChannelBuffer]): Future[JLong] =
    doRequest(ZRem(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns specified range of elements in sorted set at key
   * Elements are ordered from highest to lowest score
   * @param key, start, stop
   * @return List of elements in specified range
   */
  def zRevRange(
    key: ChannelBuffer,
    start: JLong,
    stop: JLong,
    withScores: JBoolean
  ): Future[Either[ZRangeResults, Seq[ChannelBuffer]]] =
    doRequest(ZRevRange(key, start, stop, WithScores.option(withScores)))(
      parseMBulkReply(withScores)
    )

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
    withScores: JBoolean,
    limit: Option[Limit]
  ): Future[Either[ZRangeResults, Seq[ChannelBuffer]]] =
    doRequest(ZRevRangeByScore(key, max, min, WithScores.option(withScores), limit))(
      parseMBulkReply(withScores)
    )

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

  /**
   * Gets the rank of member in the sorted set, or None if it doesn't exist, from high to low.
   * @param key
   * @param member
   * @return the rank of the member
   */
  def zRevRank(key: ChannelBuffer, member: ChannelBuffer): Future[Option[JLong]] =
    doRequest(ZRevRank(key, member)) {
      case IntegerReply(n) => Future.value(Some(n))
      case EmptyBulkReply()   => Future.value(None)
    }

  /**
   * Increment the member in sorted set key by amount.
   * Returns an option, None if the member is not found, or the set is empty, or the new value.
   * Throws an exception if the key refers to a structure that is not a sorted set.
   * @param key
   * @param amount
   * @param member
   * @return the new value of the incremented member
   */
  def zIncrBy(key: ChannelBuffer, amount: JDouble, member: ChannelBuffer): Future[Option[JDouble]] =
    doRequest(ZIncrBy(key, amount, member)) {
      case BulkReply(message) =>
        Future.value(Some(NumberFormat.toDouble(BytesToString(message.array))))
      case EmptyBulkReply()   => Future.value(None)
    }

  /**
   * Gets the rank of the member in the sorted set, or None if it doesn't exist, from low to high.
   * @param key
   * @param member
   * @return the rank of the member
   */
  def zRank(key: ChannelBuffer, member: ChannelBuffer): Future[Option[JLong]] =
    doRequest(ZRank(key, member)) {
      case IntegerReply(n) => Future.value(Some(n))
      case EmptyBulkReply()   => Future.value(None)
    }

  /**
   * Removes members from sorted set by sort order, from start to stop, inclusive.
   * @param key
   * @param start
   * @param stop
   * @return Number of members removed from sorted set.
   */
  def zRemRangeByRank(key: ChannelBuffer, start: JLong, stop: JLong): Future[JLong] =
    doRequest(ZRemRangeByRank(key, start, stop)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes members from sorted set by score, from min to max, inclusive.
   * @param key
   * @param min
   * @param max
   * @return Number of members removed from sorted set.
   */
  def zRemRangeByScore(key: ChannelBuffer, min: ZInterval, max: ZInterval): Future[JLong] =
    doRequest(ZRemRangeByScore(key, min, max)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns specified range of elements in sorted set at key.
   * Elements are ordered from lowest to highest score.
   * @param key, start, stop
   * @return ZRangeResults object containing item/score pairs
   */
  def zRange(
    key: ChannelBuffer,
    start: JLong,
    stop: JLong,
    withScores: JBoolean
  ): Future[Either[ZRangeResults, Seq[ChannelBuffer]]] =
    doRequest(ZRange(key, start, stop, WithScores.option(withScores))) {
      parseMBulkReply(withScores)
    }

}
