package com.twitter.finagle.redis

import java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BufToString, ReplyFormat}
import com.twitter.io.Buf
import com.twitter.util.Future

private[redis] trait SortedSetCommands { self: BaseClient =>

  private[this] def parseMBulkReply(
    withScores: JBoolean
  ): PartialFunction[Reply, Future[Either[ZRangeResults, Seq[Buf]]]] = {
    val parse: PartialFunction[Reply, Either[ZRangeResults, Seq[Buf]]] = {
      case MBulkReply(messages) => withScoresHelper(withScores)(messages)
      case EmptyMBulkReply => withScoresHelper(withScores)(Nil)
    }
    parse.andThen(Future.value(_))
  }

  private[this] def withScoresHelper(
    withScores: JBoolean
  )(
    messages: List[Reply]
  ): Either[ZRangeResults, Seq[Buf]] = {
    val chanBufs = ReplyFormat.toBuf(messages)
    if (withScores)
      Left(ZRangeResults(returnPairs(chanBufs)))
    else
      Right(chanBufs)
  }

  /**
   * Adds a `member` with `score` to a sorted set under the `key`.
   *
   * @return The number of elements added to sorted set.
   */
  def zAdd(key: Buf, score: JDouble, member: Buf): Future[JLong] = {
    zAddMulti(key, Seq((score, member)))
  }

  /**
   * Adds member -> score pair `members` to sorted set under the `key`.
   *
   * @note Adding multiple elements only works with redis 2.4 or later.
   *
   * @return The number of elements added to sorted set.
   */
  def zAddMulti(key: Buf, members: Seq[(JDouble, Buf)]): Future[JLong] = {
    doRequest(ZAdd(key, members.map { m => ZMember(m._1, m._2) })) {
      case IntegerReply(n) => Future.value(n)
    }
  }

  /**
   * Returns cardinality of the sorted set under the `key`, or 0
   * if `key` does not exist.
   */
  def zCard(key: Buf): Future[JLong] =
    doRequest(ZCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets number of elements in sorted set under the `key` with score
   * between `min` and `max`.
   */
  def zCount(key: Buf, min: ZInterval, max: ZInterval): Future[JLong] =
    doRequest(ZCount(key, min, max)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets member -> score pairs from sorted set under the `key` between
   * `min` and `max`. Results are limited by `limit`.
   */
  def zRangeByScore(
    key: Buf,
    min: ZInterval,
    max: ZInterval,
    withScores: JBoolean,
    limit: Option[Limit]
  ): Future[Either[ZRangeResults, Seq[Buf]]] =
    doRequest(
      ZRangeByScore(key, min, max, if (withScores) Some(WithScores) else None, limit)
    )(parseMBulkReply(withScores))

  /**
   * Removes specified `members` from sorted set at `key`.
   *
   * @return The number of members removed.
   */
  def zRem(key: Buf, members: Seq[Buf]): Future[JLong] =
    doRequest(ZRem(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns specified range (from `start` to `end`) of elements in
   * sorted set at `key`. Elements are ordered from highest to lowest score.
   */
  def zRevRange(
    key: Buf,
    start: JLong,
    stop: JLong,
    withScores: JBoolean
  ): Future[Either[ZRangeResults, Seq[Buf]]] =
    doRequest(ZRevRange(key, start, stop, if (withScores) Some(WithScores) else None))(
      parseMBulkReply(withScores)
    )

  /**
   * Returns elements in sorted set at `key` with a score between `max` and `min`.
   * Elements are ordered from highest to lowest score Results are limited by `limit`.
   */
  def zRevRangeByScore(
    key: Buf,
    max: ZInterval,
    min: ZInterval,
    withScores: JBoolean,
    limit: Option[Limit]
  ): Future[Either[ZRangeResults, Seq[Buf]]] =
    doRequest(ZRevRangeByScore(key, max, min, if (withScores) Some(WithScores) else None, limit))(
      parseMBulkReply(withScores)
    )

  /**
   * Gets the score of a `member` in sorted set at the `key`.
   */
  def zScore(key: Buf, member: Buf): Future[Option[JDouble]] =
    doRequest(ZScore(key, member)) {
      case BulkReply(message) =>
        Future.value(Some(BufToString(message).toDouble))
      case EmptyBulkReply => Future.None
    }

  /**
   * Gets the rank of a `member` in the sorted set at the `key`, or `None`
   * if it doesn't exist.
   */
  def zRevRank(key: Buf, member: Buf): Future[Option[JLong]] =
    doRequest(ZRevRank(key, member)) {
      case IntegerReply(n) => Future.value(Some(n))
      case EmptyBulkReply => Future.None
    }

  /**
   * Increments the `member` in sorted set at the `key` by a given `amount`.
   * Returns `Some` of the new value of the incremented member or `None` if
   * the member is not found or the set is empty. Throws an exception if
   * the key refers to a structure that is not a sorted set.
   */
  def zIncrBy(key: Buf, amount: JDouble, member: Buf): Future[Option[JDouble]] =
    doRequest(ZIncrBy(key, amount, member)) {
      case BulkReply(message) =>
        Future.value(Some(BufToString(message).toDouble))
      case EmptyBulkReply => Future.None
    }

  /**
   * Gets the rank of the `member` in the sorted set at the `key`, or `None`
   * if it doesn't exist.
   */
  def zRank(key: Buf, member: Buf): Future[Option[JLong]] =
    doRequest(ZRank(key, member)) {
      case IntegerReply(n) => Future.value(Some(n))
      case EmptyBulkReply => Future.None
    }

  /**
   * Removes members from sorted set at the `key` by sort order,
   * from `start` to `stop`, inclusive.
   *
   * @return The number of members removed from sorted set.
   */
  def zRemRangeByRank(key: Buf, start: JLong, stop: JLong): Future[JLong] =
    doRequest(ZRemRangeByRank(key, start, stop)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes members from sorted set at the `key` by score, from
   * `min` to `max`, inclusive.
   *
   * @return The number of members removed from sorted set.
   */
  def zRemRangeByScore(key: Buf, min: ZInterval, max: ZInterval): Future[JLong] =
    doRequest(ZRemRangeByScore(key, min, max)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns specified range (from `start` to `stop`) of elements in
   * sorted set at the `key`. Elements are ordered from lowest to
   * highest score.
   */
  def zRange(
    key: Buf,
    start: JLong,
    stop: JLong,
    withScores: JBoolean
  ): Future[Either[ZRangeResults, Seq[Buf]]] =
    doRequest(ZRange(key, start, stop, if (withScores) Some(WithScores) else None)) {
      parseMBulkReply(withScores)
    }

  /**
   * Returns keys in given set `key`, starting at `cursor`.
   */
  def zScan(key: Buf, cursor: JLong, count: Option[JLong], pattern: Option[Buf]): Future[Seq[Buf]] =
    doRequest(ZScan(key, cursor, count, pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Removes and returns up to `count` members with the lowest scores
   * in the sorted set stored at `key`.
   */
  def zPopMin(key: Buf, count: Option[JLong]): Future[Either[ZRangeResults, Seq[Buf]]] =
    doRequest(ZPopMin(key, count)) {
      parseMBulkReply(JBoolean.TRUE)
    }

  /**
   * Removes and returns up to `count` members with the highest scores
   * in the sorted set stored at `key`.
   */
  def zPopMax(key: Buf, count: Option[JLong]): Future[Either[ZRangeResults, Seq[Buf]]] =
    doRequest(ZPopMax(key, count)) {
      parseMBulkReply(JBoolean.TRUE)
    }
}
