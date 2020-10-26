package com.twitter.finagle.postgresql.types

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import com.twitter.finagle.postgresql.EmbeddedPgSqlSpec
import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlSpec
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.types.ValueReadsSpec.ToSqlString
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.specs2.matcher.describe.Diffable

/**
 * The strategy used here is to use the Postgres' ability to produce wire bytes from a SQL statement.
 * We then read those bytes and send them through ValueReads implementation to confirm that it is able
 * to deserialize the values correctly, without any of the client's machinery.
 *
 * For example, to produce the bytes for the `Int4` type:
 *
 * {{{
 *   postgres=# SELECT int4send('1234'::int4);
 *   int4send
 * ------------
 *  \x000004d2
 * (1 row)
 * }}}
 *
 * The resulting value (`\x000004d2`) is a hexadecimal string representation of the bytes that will be present on the wire.
 * We use jdbc to execute the statement, extract the bytes and then we send those bytes into `ValueReads`
 * and confirm that we read back the original value.
 *
 * NOTE: because of the type cast from string, there are a few caveats:
 *
 * - the string representation must escape single quotes, e.g.: "My name's Bob" -> "My name''s Bob"
 * - the `ToSqlString` trait is necessary to handle types that require finer control than `.toString`
 */
class ValueReadsSpec extends PgSqlSpec with EmbeddedPgSqlSpec with PropertiesSpec {

  // The function to convert a type to its wire representation is mostly guessable from its name, but not always.
  // This maps types to custom names, otherwise, we use the typical naming scheme.
  // NOTE: we can extract the function name from the pg_type.dat file, but let's not add this to PgType if not necessary.
  val customFuncs = Map(
    PgType.Numeric -> "numeric_send",
    PgType.Timestamptz -> "timestamptz_send",
    PgType.Timestamp -> "timestamp_send",
    PgType.Uuid -> "uuid_send"
  )
  def sendFunc(tpe: PgType) =
    customFuncs.getOrElse(tpe, s"${tpe.name}send")

  def pgBytes(statement: String) =
    withStatement { stmt =>
      using(stmt.executeQuery(statement)) { rs =>
        require(rs.next, "no result in result set")
        val hex = rs.getString(1)
        // format is `\xcafe01234
        val bytes = hex.drop(2).grouped(2).map { byte =>
          java.lang.Integer.parseInt(byte, 16).toByte
        }.toArray

        Buf.ByteArray.Owned(bytes)
      }
    }

  def pgBytes[T](pgType: PgType, value: T)(implicit toSqlString: ToSqlString[T]): Buf =
    // e.g.: `SELECT int4send('1234'::int4)`
    pgBytes(s"SELECT ${sendFunc(pgType)}('${toSqlString.toString(value)}'::${pgType.name});")

  def pgArrayBytes[T](pgType: PgType, values: List[T])(implicit toSqlString: ToSqlString[T]): Buf = {
    // e.g.: `SELECT array_send('{1,2,3,4}'::int4[])`
    val arrStr = values.map(v => toSqlString.toString(v)).map(v => s"""'$v'""").mkString("ARRAY[", ",", "]")
    pgBytes(s"SELECT array_send($arrStr::${pgType.name}[]);")
  }

  def readFragment[T: Arbitrary: Diffable: ToSqlString](valueReads: ValueReads[T], tpe: PgType) =
    s"successfully read value of type ${tpe.name}" in prop { value: T =>
      val bytes = pgBytes(tpe, value)
      val read = valueReads.reads(tpe, WireValue.Value(bytes), StandardCharsets.UTF_8)
      read.asScala must beSuccessfulTry(be_===(value)) // === delegates to Diffable
    }

  def arrayFragment[T: Arbitrary: Diffable: ToSqlString](valueReads: ValueReads[T], arrayType: PgType, tpe: PgType) =
    s"successfully read one-dimensional array of values of type ${tpe.name}" in prop { values: List[T] =>
      val bytes = pgArrayBytes(tpe, values)
      val arrayReads = ValueReads.traversableReads[List, T](valueReads, implicitly)
      val read = arrayReads.reads(arrayType, WireValue.Value(bytes), StandardCharsets.UTF_8)
      read.asScala must beSuccessfulTry(be_===(values)) // === delegates to Diffable
    }

  def simpleSpec[T: Arbitrary: Diffable: ToSqlString](valueReads: ValueReads[T], pgType: PgType*) = {
    val fs = pgType
      .flatMap { tpe =>
        lazy val af = PgType.arrayOf(tpe).map { arrayType =>
          arrayFragment(valueReads, arrayType, tpe)
        }

        readFragment(valueReads, tpe) :: af.toList
      }

    fragments(fs)
  }

  def failFor(valueReads: ValueReads[_], s: String, tpe: PgType) =
    s"fail for $s" in {
      val bytes = pgBytes(tpe, s)
      val read = valueReads.reads(tpe, WireValue.Value(bytes), StandardCharsets.UTF_8)
      read.asScala must beAFailedTry(beAnInstanceOf[PgSqlClientError])
    }

  "ValueReads" should {
    "readsBigDecimal" should simpleSpec(ValueReads.readsBigDecimal, PgType.Numeric)
    "readsBigDecimal" should failFor(ValueReads.readsBigDecimal, "NaN", PgType.Numeric)
    "readsBool" should simpleSpec(ValueReads.readsBoolean, PgType.Bool)
    "readsBuf" should simpleSpec(ValueReads.readsBuf, PgType.Bytea)
    // TODO: figure out why charsend(character) doesn't work
//    "readsByte" should simpleSpec(ValueReads.readsByte, PgType.Char)
    "readsDouble" should simpleSpec(ValueReads.readsDouble, PgType.Float8)
    "readsFloat" should simpleSpec(ValueReads.readsFloat, PgType.Float4)
    "readsInstant" should simpleSpec(ValueReads.readsInstant, PgType.Timestamptz, PgType.Timestamp)
    "readsInstant" should {
      failFor(ValueReads.readsInstant, "Infinity", PgType.Timestamptz)
      failFor(ValueReads.readsInstant, "-Infinity", PgType.Timestamptz)
    }
    "readsInt" should simpleSpec(ValueReads.readsInt, PgType.Int4)
    "readsLong" should simpleSpec(ValueReads.readsLong, PgType.Int8)
    "readsShort" should simpleSpec(ValueReads.readsShort, PgType.Int2)
    "readsString" should {
      // "The character with the code zero cannot be in a string constant."
      // https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
      val genNonZeroByte = implicitly[Arbitrary[List[Char]]].arbitrary
        .map(_.mkString)
        .suchThat(str => !str.getBytes("UTF8").contains(0))
      implicit val noZeroByteString: Arbitrary[String] = Arbitrary(genNonZeroByte)
      simpleSpec(ValueReads.readsString, PgType.Text, PgType.Varchar, PgType.Bpchar, PgType.Unknown)
    }
    "readsString" should {
      // names are limited to ascii, 63 bytes long
      implicit val nameString: Arbitrary[String] = Arbitrary(Gen.listOfN(63, genAsciiChar).map(_.mkString))
      simpleSpec(ValueReads.readsString, PgType.Name)
    }
    "readsUuid" should simpleSpec(ValueReads.readsUuid, PgType.Uuid)
  }
}

object ValueReadsSpec {
  trait ToSqlString[T] {
    def toString(value: T): String
  }

  object ToSqlString {

    def escape(value: String) = value.replaceAllLiterally("'", "''")

    implicit def fromToString[T]: ToSqlString[T] = new ToSqlString[T] {
      override def toString(value: T): String = ToSqlString.escape(value.toString)
    }

    implicit val bufToSqlString: ToSqlString[Buf] = new ToSqlString[Buf] {
      def hex(arr: Array[Byte]): String = {
        val h = arr.map(s => f"$s%02X").mkString
        s"\\x$h"
      }
      override def toString(value: Buf): String =
        hex(Buf.ByteArray.Shared.extract(value))
    }

    implicit val instantToSqlString: ToSqlString[Instant] = new ToSqlString[Instant] {

      // Postgres says they allow reading ISO 8601 strings, but it's not quite the case.
      // ISO 8601 allows prefixing the year with a + or - to disambiguate years before 0000 and after 9999
      // https://en.wikipedia.org/wiki/ISO_8601#Years
      // Postgres wants AD/BC instead.
      // Note that this also means that year "-1" is 2 BC.
      val fmt = DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS'Z' GG")
        .withZone(ZoneId.of("UTC"))

      override def toString(value: Instant): String = {
        val str = fmt.format(value)
        str.charAt(0) match {
          case '+' | '-' => str.drop(1).mkString
          case _ => str
        }
      }
    }
  }
}
