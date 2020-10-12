package com.twitter.finagle.postgresql.types

import java.nio.charset.StandardCharsets

import com.twitter.finagle.postgresql.EmbeddedPgSqlSpec
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
 *
 */
class ValueReadsSpec extends PgSqlSpec with EmbeddedPgSqlSpec with PropertiesSpec {

  def pgBytes[T](pgType: PgType, value: T)(implicit toSqlString: ToSqlString[T]): Buf = {
    withStatement { stmt =>
      using(stmt.executeQuery(s"SELECT ${pgType.name}send('${toSqlString.toString(value)}'::${pgType.name});")) { rs =>
        require(rs.next, "no result in result set")
        val hex = rs.getString(1)
        // format is `\xcafe01234
        val bytes = hex.drop(2).grouped(2).map { byte =>
          java.lang.Integer.parseInt(byte, 16).toByte
        }.toArray

        Buf.ByteArray.Owned(bytes)
      }
    }
  }

  def fragment[T: Arbitrary: Diffable: ToSqlString](valueReads: ValueReads[T], pgType: PgType*) = {
    val typeFragments = pgType.map { tpe =>
      s"successfully read value from ${tpe.name}" in prop { value: T =>
        val bytes = pgBytes(tpe, value)
        val read = valueReads.reads(tpe, WireValue.Value(bytes), StandardCharsets.UTF_8)
        read.asScala must beSuccessfulTry(be_===(value)) // === delegates to Diffable
      }
    }

    fragments(typeFragments)
  }

  "ValueReads" should {
    "readsBool" should fragment(ValueReads.readsBoolean, PgType.Bool)
    // TODO: figure out why charsend(character) doesn't work
//    "readsByte" should fragment(ValueReads.readsByte, PgType.Char)
    "readsShort" should fragment(ValueReads.readsShort, PgType.Int2)
    "readsInt" should fragment(ValueReads.readsInt, PgType.Int4)
    "readsLong" should fragment(ValueReads.readsLong, PgType.Int8)
    "readsBuf" should fragment(ValueReads.readsBuf, PgType.Bytea)
    "readsString" should {
      // "The character with the code zero cannot be in a string constant."
      // https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
      implicit val noZeroByteString = implicitly[Arbitrary[String]].arbitrary
        .suchThat(str => !str.getBytes("UTF8").contains(0))
      fragment(ValueReads.readsString, PgType.Text, PgType.Varchar, PgType.Bpchar, PgType.Unknown)
        .append {
          implicit val nameString = Arbitrary(
            Gen.listOf(Gen.choose(32.toChar, 126.toChar))
            .map(_.mkString)
            .suchThat(_.length < 64)
          ) // names are 63 bytes long
          fragment(ValueReads.readsString, PgType.Name)
        }
    }
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
      override def toString(value: Buf): String = {
        hex(Buf.ByteArray.Shared.extract(value))
      }
    }
  }
}
