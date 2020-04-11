package com.twitter.finagle.postgres.values

import java.text.NumberFormat
import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.ChronoField

import io.netty.buffer.{ByteBuf, Unpooled}

private object DateTimeUtils {
  val POSTGRES_EPOCH_MICROS = 946684800000000L
  val ZONE_REGEX = "(.*)(-|\\+)([0-9]{2})".r

  private val timeTzParser = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_TIME)
    .optionalStart()
    .appendOffset("+HH:MM:ss", "Z")
    .optionalEnd()
    .optionalStart()
    .appendOffset("+HH:mm", "Z")
    .optionalEnd()
    .optionalStart()
    .appendOffset("+HH", "Z")
    .optionalEnd()
    .toFormatter

  /**
    * https://www.postgresql.org/docs/current/datatype-datetime.html
    *
    * ISO 8601 specifies the use of uppercase letter T to separate the date and time.
    * PostgreSQL accepts that format on input, but on output it uses a space rather than T, as shown above.
    * This is for readability and for consistency with RFC 3339 as well as some other database systems.
    */
  private val PostgresDateTimeFormat = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_DATE)
    .appendLiteral(' ')
    .append(DateTimeFormatter.ISO_LOCAL_TIME)
    .optionalStart()
    .appendOffsetId()
    .toFormatter()

  def format(i: Instant) = PostgresDateTimeFormat.withZone(ZoneOffset.UTC).format(i)
  def format(zdt: ZonedDateTime) = PostgresDateTimeFormat.format(zdt)
  def format(ldt: LocalDateTime) = PostgresDateTimeFormat.format(ldt)

  private val PostgresDateTimeParser = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_DATE)
    .appendLiteral(' ')
    .append(timeTzParser)
    .toFormatter()

  def parseLocalDateTime(s: String) = PostgresDateTimeParser.parse(s, LocalDateTime.from(_))
  def parseInstant(s: String) = PostgresDateTimeParser.withZone(ZoneOffset.UTC).parse(s, Instant.from(_))
  def parseZonedDateTime(s: String) = PostgresDateTimeParser.parse(s, ZonedDateTime.from(_))

  def readTimestamp(buf: ByteBuf) = {
    val micros = buf.readLong() + POSTGRES_EPOCH_MICROS
    val seconds = micros / 1000000L
    val nanos = (micros - seconds * 1000000L) * 1000
    Instant.ofEpochSecond(seconds, nanos)
  }

  def readTimeTz(buf: ByteBuf) = {
    val time = LocalTime.ofNanoOfDay(buf.readLong() * 1000)
    val zone = ZoneOffset.ofTotalSeconds(-buf.readInt())
    time.atOffset(zone)
  }

  def readInterval(buf: ByteBuf) = {
    val micros = buf.readLong()
    val days = buf.readInt()
    val months = buf.readInt()
    Interval(Duration.ofNanos(micros * 1000), Period.ofMonths(months).plusDays(days))
  }

  def parseTimeTz(str: String) = OffsetTime.parse(str, timeTzParser)

  def writeInstant(instant: Instant) = {
    val seconds = instant.getEpochSecond
    val micros = instant.getLong(ChronoField.MICRO_OF_SECOND) + seconds * 1000000
    val buf = Unpooled.buffer(8)
    buf.writeLong(micros - POSTGRES_EPOCH_MICROS)
    buf
  }

  def writeTimestamp(timestamp: LocalDateTime) = writeInstant(timestamp.atOffset(ZoneOffset.UTC).toInstant)

  def writeTimestampTz(timestamp: ZonedDateTime) = writeInstant(timestamp.toInstant)

  def writeTimeTz(time: OffsetTime) = {
    val buf = Unpooled.buffer(12)
    buf.writeLong(time.toLocalTime.toNanoOfDay / 1000)
    buf.writeInt(-time.getOffset.getTotalSeconds)
    buf
  }

  def writeInterval(interval: Interval) = {
    val buf = Unpooled.buffer(16)
    buf.writeLong(interval.timeDifference.getSeconds * 1000000 + interval.timeDifference.getNano / 1000)
    buf.writeInt(interval.dateDifference.getDays)
    buf.writeInt(interval.dateDifference.getYears * 12 + interval.dateDifference.getMonths)
    buf
  }
}

// Java time doesn't have the same notion of Interval that Postgres has
// The simplest way to model it is by capturing the time portion and date portion separately (as postgres does)
// TODO: this could probably implement Temporal to unify with java.time API
case class Interval(timeDifference: Duration, dateDifference: Period) {
  override def toString: String = {
    val timePart = Option(timeDifference).filter(_.getSeconds != 0).map {
      t =>
        val sign = if(t.getSeconds < 0) "-" else "+"
        val totalSeconds = Math.abs(t.getSeconds)
        val totalMinutes = totalSeconds / 60
        val totalHours = totalMinutes / 60
        val seconds = (totalSeconds % 60).toString.reverse.padTo(2, '0').reverse
        val minutes = (totalMinutes - totalHours * 60).toString.reverse.padTo(2, '0').reverse
        val hours = totalHours.toString.reverse.padTo(2, '0').reverse
        val micros = (t.getNano / 1000).toString.reverse.padTo(6, '0').reverse
        s"$sign$hours:$minutes:$seconds.$micros"
    }

    val datePart = Option(dateDifference).filterNot(dd => dd.getDays == 0 && dd.getMonths == 0 && dd.getYears == 0).map {
      d =>
        def sign(i: Int) = if(i > 0) s"+$i" else i.toString
        val years = Option(d.getYears).filter(_ != 0).map(y => s"${sign(y)} years")
        val months = Option(d.getMonths).filter(_ != 0).map(m => s"${sign(m)} mons")
        val days = Option(d.getDays).filter(_ != 0).map(d => s"${sign(d)} days")
        List(years, months, days).flatten.mkString(" ")
    }

    List(datePart, timePart).flatten.mkString(" ")
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case Interval(td, dd) =>
      val e = td.getSeconds == timeDifference.getSeconds && td.getNano == timeDifference.getNano &&
      dd.getYears * 12 + dd.getMonths == dateDifference.getYears * 12 + dateDifference.getMonths &&
      dd.getDays == dateDifference.getDays
      if(!e) {
        println("debug")
      }
      e
    case _ => false
  }
}

object Interval {
  val regex = """(?:([\-+]?\d+) years? )?(?:([\-+]?\d+) mons? )?(?:([\-+]?\d+) days? )?(?:(-|\+)?(\d{2}):(\d{2}):(\d{2})(\.\d+)?)?""".r
  def parse(s: String) = {
    val t = regex.findFirstMatchIn(s).map {
      m =>
        val period = Period.ofYears(Option(m.group(1)).map(_.toInt).getOrElse(0))
          .plusMonths(Option(m.group(2)).map(_.toLong).getOrElse(0))
          .plusDays(Option(m.group(3)).map(_.toLong).getOrElse(0))
        val duration = if(m.group(4) == "-") {
          Duration.ofHours(Option(m.group(5)).map(h => -1 * h.toLong).getOrElse(0))
            .minusMinutes(Option(m.group(6)).map(_.toLong).getOrElse(0))
            .minusSeconds(Option(m.group(7)).map(_.toLong).getOrElse(0))
            .minusNanos(Option(m.group(8)).map(BigDecimal.apply).map(_ * 1000000000L).map(_.toLong).getOrElse(0))
        } else {
          Duration.ofHours(Option(m.group(5)).map(_.toLong).getOrElse(0))
            .plusMinutes(Option(m.group(6)).map(_.toLong).getOrElse(0))
            .plusSeconds(Option(m.group(7)).map(_.toLong).getOrElse(0))
            .plusNanos(Option(m.group(8)).map(BigDecimal.apply).map(_ * 1000000000L).map(_.toLong).getOrElse(0))
        }
        Interval(duration, period)
    }
    t.getOrElse(throw new DateTimeException("Interval could not be parsed"))
  }
}
