package com.twitter.finagle.postgresql.types

import com.twitter.finagle.postgresql.Client
import com.twitter.finagle.postgresql.Parameter
import com.twitter.finagle.postgresql.PgSqlIntegrationSpec
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.util.Await
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.specs2.matcher.describe.Diffable

/**
 * The strategy used here is to issue a "typed" select statement of the form
 *
 * {{{
 *   SELECT $1::"int4"
 * }}}
 *
 * And supply the value as a statement parameter and then read the value back.
 *
 * NOTE: the double quotes around the type name is required due to the "char" (OID 18) type which conflicts
 * with the "bpchar" type alias, i.e.: char(n). https://stackoverflow.com/a/42484838
 *
 * Unfortunately, this relies on a lot of other machinery to work correctly, namely:
 *
 * * rich client
 * * prepared statements
 * * [[ValueReads]] must also exist for the corresponding T
 *
 * Another approach would be to write the value to a table and read it back using JDBC.
 * But this would make it difficult to compare the read value since we'd have to go through
 * Java types.
 */
class ValueWritesSpec extends PgSqlIntegrationSpec with PropertiesSpec {

  // Issues the "typed" select statement and read the value back.
  def writeAndRead[T: ValueReads](client: Client, value: T, valueWrites: ValueWrites[T], tpe: PgType) = {
    val prepared = client.prepare(Name.Unnamed, s"""SELECT $$1::"${tpe.name}"""")
    Await.result(
      prepared.select(Parameter(value)(valueWrites) :: Nil)(_.get[T](0))
        .map(_.head)
    )
  }

  def writeFragment[T: Arbitrary: Diffable: ValueReads](valueWrites: ValueWrites[T], tpe: PgType) =
    s"successfully writes value of type ${tpe.name}" in withRichClient() { client =>
      prop { value: T =>
        writeAndRead(client, value, valueWrites, tpe) must beIdentical(value)
      }
    }

  def arrayFragment[T: Arbitrary: Diffable: ValueReads](valueWrites: ValueWrites[T], arrayType: PgType, tpe: PgType) =
    s"successfully read one-dimensional array of values of type ${tpe.name}" in withRichClient() { client =>
      prop { values: List[T] =>
        val arrayWrites = ValueWrites.traversableWrites[List, T](valueWrites)
        writeAndRead(client, values, arrayWrites, arrayType) must beIdentical(values)
      }
    }.setGen(Gen.listOfN(5, Arbitrary.arbitrary[T])) // limit to up to 5 values

  def simpleSpec[T: Arbitrary: Diffable: ValueReads](valueWrites: ValueWrites[T], pgType: PgType*) = {
    val fs = pgType
      .flatMap { tpe =>
        lazy val af = PgType.arrayOf(tpe).map { arrayType =>
          arrayFragment(valueWrites, arrayType, tpe)
        }

        writeFragment(valueWrites, tpe) :: af.toList
      }

    fragments(fs)
  }

  "ValueWrites" should {
    "writesBigDecimal" should simpleSpec(ValueWrites.writesBigDecimal, PgType.Numeric)
    "writesBool" should simpleSpec(ValueWrites.writesBoolean, PgType.Bool)
    "writesBuf" should simpleSpec(ValueWrites.writesBuf, PgType.Bytea)
    "writesByte" should simpleSpec(ValueWrites.writesByte, PgType.Char)
    "writesDouble" should simpleSpec(ValueWrites.writesDouble, PgType.Float8)
    "writesFloat" should simpleSpec(ValueWrites.writesFloat, PgType.Float4)
    "writesInet" should simpleSpec(ValueWrites.writesInet, PgType.Inet)
    "writesInstant" should simpleSpec(ValueWrites.writesInstant, PgType.Timestamptz, PgType.Timestamp)
    "writesInt" should simpleSpec(ValueWrites.writesInt, PgType.Int4)
    "writesJson" should simpleSpec(ValueWrites.writesJson, PgType.Json, PgType.Jsonb)
    "writesLong" should simpleSpec(ValueWrites.writesLong, PgType.Int8)
    "writesShort" should simpleSpec(ValueWrites.writesShort, PgType.Int2)
    "writesString" should {
      // "The character with the code zero cannot be in a string constant."
      // https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
      val genNonZeroByte = implicitly[Arbitrary[List[Char]]].arbitrary
        .map(_.mkString)
        .suchThat(str => !str.getBytes("UTF8").contains(0))
      implicit val noZeroByteString: Arbitrary[String] = Arbitrary(genNonZeroByte)

      val types = PgType.Text :: PgType.Varchar :: PgType.Bpchar :: Nil
      val compatTypes = if (tag != "9") PgType.Unknown :: types else types
      simpleSpec(ValueWrites.writesString, compatTypes: _*)
    }
    "writesString" should {
      // names are limited to ascii, 63 bytes long
      implicit val nameString: Arbitrary[String] = Arbitrary(Gen.listOfN(63, genAsciiChar).map(_.mkString))
      simpleSpec(ValueWrites.writesString, PgType.Name)
    }
    "writesString" should {
      implicit val jsonString: Arbitrary[String] = Arbitrary(genJsonString.map(_.value))
      simpleSpec(ValueWrites.writesString, PgType.Json)
    }
    "writesUuid" should simpleSpec(ValueWrites.writesUuid, PgType.Uuid)
  }
}
