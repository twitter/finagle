package com.twitter.finagle.postgres

import java.nio.charset.StandardCharsets
import java.time.{ZonedDateTime, _}
import java.time.temporal.JulianFields
import java.util.UUID

import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import com.twitter.finagle.postgres.values.Interval

object Generators {
  //need a more sensible BigDecimal generator, because ScalaCheck goes crazy with it and we can't even stringify them
  //this will be sufficient to test the decoder
  implicit val arbBD: Arbitrary[BigDecimal] = Arbitrary(for {
    precision <- Gen.choose(1, 32)
    scale <- Gen.choose(-precision, precision)
    digits <- Gen.listOfN[Char](precision, Gen.numChar)
  } yield BigDecimal(BigDecimal(digits.mkString).bigDecimal.movePointLeft(scale)))

  implicit val arbDate = Arbitrary[LocalDate](for {
    julian <- Gen.choose(1721060, 5373484)  //Postgres date parser doesn't like dates outside year range 0000-9999
  } yield LocalDate.now().`with`(JulianFields.JULIAN_DAY, julian))

  implicit val arbTime: Arbitrary[LocalTime] = Arbitrary[LocalTime](for {
    usec <- Gen.choose(0L, 24L * 60 * 60 * 1000000 - 1)
  } yield LocalTime.ofNanoOfDay(usec * 1000))

  implicit val arbInstant = Arbitrary[Instant](for {
    milli <- Gen.posNum[Long]
  } yield Instant.ofEpochMilli(milli))

  implicit val arbTimestamp = Arbitrary[LocalDateTime](for {
    milli <- Gen.posNum[Long]
  } yield LocalDateTime.ofInstant(Instant.ofEpochMilli(milli), ZoneId.systemDefault()))

  implicit val arbTimestampTz = Arbitrary[ZonedDateTime](for {
    milli <- Gen.posNum[Long]
  } yield ZonedDateTime.ofInstant(Instant.ofEpochMilli(milli), ZoneId.systemDefault()))

  implicit val arbZoneOffset = Arbitrary(Gen.choose(-12, 12).map(ZoneOffset.ofHours))

  implicit val arbInterval = Arbitrary(for {
    months <- Gen.choose(-120, 120)
    years <- Gen.choose(-10, 10)
    days <- Gen.choose(-50, 50)
    hours <- Gen.choose(-50, 50)
    minutes <- Gen.choose(0, 59)
    seconds <- Gen.choose(0, 59)
  } yield Interval(
    Duration.ofSeconds(seconds).plusMinutes(minutes).plusHours(hours),
    Period.ofMonths(months).plusYears(years).plusDays(days)
  ))

  implicit val arbTimeTz = Arbitrary[OffsetTime](for {
    time <- arbitrary[LocalTime]
    offs <- arbitrary[ZoneOffset]
  } yield time.atOffset(offs))

  implicit val arbUUID = Arbitrary[UUID](Gen.uuid)

  // arbitrary string that only contains valid UTF-8 characters
  implicit val arbString = {
    val encoder = StandardCharsets.UTF_8.newEncoder()
    val chars = Arbitrary.arbChar.arbitrary.suchThat(ch => ch.toInt != 0 && encoder.canEncode(ch))
    Arbitrary[String](Gen.listOf(chars).map(_.mkString))
  }

  // TODO: can empty maps be supported?
  implicit val arbHStore: Arbitrary[Map[String, Option[String]]] = Arbitrary(
    Gen.mapOf(for {
      k <- Gen.identifier
      v <- Gen.oneOf(Gen.alphaStr.map(Some(_)), Gen.const(None))
    } yield (k, v)).suchThat(_.nonEmpty)
  )

  // postgres has slightly different precision rules, but that doesn't mean the decoder isn't working
  implicit val arbFloat = Arbitrary[Float](for {
    precision <- Gen.choose(1, 6)
    scale <- Gen.choose(-10, 10)
    digits <- Gen.listOfN[Char](precision, Gen.numChar)
  } yield BigDecimal(BigDecimal(digits.mkString).bigDecimal.movePointLeft(scale)).toFloat)

  implicit val arbDouble = Arbitrary[Double](for {
    precision <- Gen.choose(1, 15)
    scale <- Gen.choose(-20, 20)
    digits <- Gen.listOfN[Char](precision, Gen.numChar)
  } yield BigDecimal(BigDecimal(digits.mkString).bigDecimal.movePointLeft(scale)).toDouble)
}
