package com.twitter.finagle.postgresql.types

import java.time.LocalDate
import java.time.temporal.ChronoUnit

/**
 * Some utilities to deal with Postgres' internal date representations and boundaries.
 */
object PgDate {

  /**
   * The Postgres Epoch's local date, i.e.: 2000-01-01
   */
  val Epoch: LocalDate = LocalDate.parse("2000-01-01")

  /**
   * The minimum date value Postgresql can represent.
   *
   * NOTE: this happens to be the Julian calendar epoch, November 24, 4714 BC or -4713-11-24 in ISO
   * NOTE: experimentation has shown that it actually supports up to -4713-12-31T23:59:59.999999
   */
  val Min: LocalDate = LocalDate.parse("-4713-11-24")

  /**
   * The maximum date value Postgresql can represent.
   */
  val Max: LocalDate = LocalDate.parse("+5874897-12-31")

  /**
   * Convert a day offset from [[Epoch]] into a [[java.time.LocalDate]]
   *
   * @param daysFromEpoch the number of days from the epoch (may be negative)
   * @return the [[java.time.LocalDate]] represented by the provided offset
   */
  def dayOffsetAsLocalDate(daysFromEpoch: Int): LocalDate =
    Epoch.plusDays(daysFromEpoch.toLong)

  /**
   * Convert a [[java.time.LocalDate]] into a number of days offset from [[Epoch]]
   *
   * @param localDate the date to convert
   * @return the number of days offset from [[Epoch]]
   */
  def localDateAsEpochDayOffset(localDate: LocalDate): Int = {
    require(localDate.isAfter(Min) || localDate.isEqual(Min))
    require(localDate.isBefore(Max) || localDate.isEqual(Max))
    ChronoUnit.DAYS.between(Epoch, localDate).toInt
  }
}
