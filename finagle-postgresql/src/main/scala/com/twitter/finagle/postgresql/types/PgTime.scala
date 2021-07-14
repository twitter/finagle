package com.twitter.finagle.postgresql.types

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Some utilities to deal with Postgres' internal timestamp representations and boundaries.
 */
object PgTime {

  /**
   * Postgresql's Epoch is 2000-01-01T00:00:00Z
   * Timestamp and TimestampTz store microseconds since this epoch as an 8-byte integer.
   *
   * NOTE: postgres used to use a floating point to represent this offset.
   * To determine what internal representation it uses, use `SHOW integer_datetimes;` and this should print `on`.
   * This parameter is set at compile time and cannot be changed later on.
   */
  val Epoch: Instant = Instant.parse("2000-01-01T00:00:00Z")

  /**
   * The minimum timestamp value Postgresql can represent.
   *
   * NOTE: this happens to be the Julian calendar epoch, November 24, 4714 BC or -4713-11-24 in ISO
   * NOTE: experimentation has shown that it actually supports up to -4713-12-31T23:59:59.999999
   */
  val Min: Instant = Instant.parse("-4713-11-24T00:00:00Z")

  /**
   * The maximum timestamp value Postgresql can represent
   */
  val Max: Instant = Instant.parse("+294276-12-31T23:59:59.999999Z")

  /**
   * Converts a timestamp value to a [[java.time.Duration]] from the Postgres' epoch.
   * Postgres timestamps are stored as microseconds since its epoch.
   * The value can be negative.
   */
  def usecOffsetAsDuration(usecFromEpoch: Long): Duration =
    Duration.of(usecFromEpoch, ChronoUnit.MICROS)

  /**
   * Converts [[java.time.Duration]] to an offset (timestamp) from Postgres' epoch.
   * Postgres timestamps are stored as microseconds since its epoch.
   * The value can be negative.
   */
  def durationAsUsecOffset(duration: Duration): Long =
    duration.getSeconds * 1000000 + duration.getNano / 1000

  /**
   * Converts a timestamp value to a [[java.time.Instant]].
   * Postgres timestamps are stored as microseconds since its epoch.
   * The value can be negative.
   */
  def usecOffsetAsInstant(usecFromEpoch: Long): Instant =
    Epoch.plus(usecOffsetAsDuration(usecFromEpoch))

  /**
   * Converts a [[java.time.Instant]] to a timestamp value.
   * Postgres timestamps are stored as microseconds since its epoch.
   * The value can be negative.
   */
  def instantAsUsecOffset(instant: java.time.Instant): Long =
    durationAsUsecOffset(Duration.between(Epoch, instant))
}
