package com.twitter.finagle.redis

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.net.InetSocketAddress
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future, Time}

private[redis] trait KeyCommands { self: BaseClient =>
  import com.twitter.conversions.DurationOps._

  /**
   * Removes keys
   * @param keys list of keys to remove
   * @return Number of keys removed
   */
  def dels(keys: Seq[Buf]): Future[JLong] =
    doRequest(Del(keys)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Serialize the value stored at key in a Redis-specific format and
   * returns it to the user
   * @param key
   * @return bytes, or none if the key did not exist
   */
  def dump(key: Buf): Future[Option[Buf]] =
    doRequest(Dump(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  /**
   * Checks if given key exists
   * @param key
   * @return true if key exists, false otherwise
   */
  def exists(key: Buf): Future[JBoolean] =
    doRequest(Exists(key)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Sets how long it will take the key to expire
   * @param key
   * @param ttl
   * @return boolean, true if it successfully set the ttl (time to live) on a valid key,
   * false otherwise.
   */
  def expire(key: Buf, ttl: JLong): Future[JBoolean] =
    doRequest(Expire(key, ttl)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Same effect and semantic as "expire", but takes an absolute Unix timestamp
   * @param key
   * @param ttl time-to-live as unix timestamp
   * @return boolean, true if it successfully set the ttl (time to live) on a valid key,
   * false otherwise.
   */
  def expireAt(key: Buf, ttl: JLong): Future[JBoolean] =
    doRequest(ExpireAt(key, Time.fromMilliseconds(ttl))) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Returns all keys matching pattern
   * @param pattern
   * @return list of keys matching pattern
   */
  def keys(pattern: Buf): Future[Seq[Buf]] =
    doRequest(Keys(pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Migrates all keys to the destination server
   * @param destAddr target redis server
   * @param keys list of keys to be migrated
   * @param timeout timeout before failing, defaults to 5 seconds
   * @return unit
   */
  def migrate(
    destAddr: InetSocketAddress,
    keys: Seq[Buf],
    timeout: Duration = 5.seconds
  ): Future[Unit] = {
    doRequest(Migrate(destAddr, keys, timeout)) {
      case StatusReply(_) => Future.Unit
      case ErrorReply(msg) => Future.exception(new ServerError(msg))
    }
  }

  /**
   * Move key from the currently selected database to the specified destination
   * database. When key already exists in the destination database, or it does
   * not exist in the source database, it does nothing.
   *
   * @param key, db
   * @return true if key was moved.
   *         false if key was not moved for any reason.
   */
  def move(key: Buf, db: Buf): Future[JBoolean] =
    doRequest(Move(key, db)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Set a key's time to live in milliseconds.
   *
   * @param key, milliseconds
   * @return true if the timeout was set.
   *         false if key does not exist or the timeout could not be set.
   * @see https://redis.io/commands/pexpire
   */
  def pExpire(key: Buf, milliseconds: JLong): Future[JBoolean] =
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
   * @see https://redis.io/commands/pexpireat
   */
  def pExpireAt(key: Buf, timestamp: JLong): Future[JBoolean] =
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
  def pTtl(key: Buf): Future[Option[JLong]] =
    doRequest(PTtl(key)) {
      case IntegerReply(n) =>
        // -2 indicates key doesn't exist, -1 is key exists but no ttl set.
        if (n != -2) Future.value(Some(n))
        else Future.value(None)
    }

  /**
   * Returns keys starting at cursor
   * @param cursor, count, pattern
   * @return cursor followed by matching keys
   */
  def scans(cursor: JLong, count: Option[JLong], pattern: Option[Buf]): Future[Seq[Buf]] =
    doRequest(Scan(cursor, count, pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Gets the ttl of the given key.
   * @param key
   * @return Option containing either the ttl in seconds if the key exists
   * and has a timeout, or else nothing.
   */
  def ttl(key: Buf): Future[Option[JLong]] =
    doRequest(Ttl(key)) {
      case IntegerReply(n) =>
        if (n != -1) Future.value(Some(n))
        else Future.None
    }

  /**
   * Persist a key by removing it's expiration time
   * @param key
   * @return 1 if ttl was removed, 0 if key doesn't exist or doesn't have a ttl
   */
  def persist(key: Buf): Future[JLong] =
    doRequest(Persist(key)) {
      case IntegerReply(n) => Future.value(n)
    }
}
