package com.twitter.finagle.postgresql

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.temporal.ChronoUnit

import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.Field
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Numeric
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.PgArray
import com.twitter.finagle.postgresql.Types.PgArrayDim
import com.twitter.finagle.postgresql.Types.Timestamp
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.types.Json
import com.twitter.finagle.postgresql.types.PgNumeric
import com.twitter.finagle.postgresql.types.PgTime
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.specs2.ScalaCheck
import org.specs2.matcher.describe.ComparisonResult
import org.specs2.matcher.describe.Diffable

trait PropertiesSpec extends ScalaCheck {

  case class AsciiString(value: String)
  lazy val genAsciiChar: Gen[Char] = Gen.choose(32.toChar, 126.toChar)
  lazy val genAsciiString: Gen[AsciiString] = Gen.listOf(genAsciiChar).map(_.mkString).map(AsciiString)
  implicit lazy val arbAsciiString: Arbitrary[AsciiString] = Arbitrary(genAsciiString)

  case class JsonString(value: String)
  // TODO
  lazy val genJsonString: Gen[JsonString] = Gen.const("""{"valid": true, "b": 1.4, "array": [1,2,3]}""").map(JsonString)
  implicit lazy val arbJsonString: Arbitrary[JsonString] = Arbitrary(genJsonString)
  lazy val genJson: Gen[Json] = genJsonString.map(str => Json(Buf.Utf8(str.value), StandardCharsets.UTF_8))
  implicit lazy val arbJson: Arbitrary[Json] = Arbitrary(genJson)

  // TODO: Once we have actual data types, Gen.oneOf(...)
  implicit lazy val arbOid = Arbitrary(Gen.chooseNum(0, 0xffffffffL).map(Oid))

  val genParameter: Gen[BackendMessage.Parameter] = Gen.oneOf(
    BackendMessage.Parameter.ServerVersion,
    BackendMessage.Parameter.ServerEncoding,
    BackendMessage.Parameter.ClientEncoding,
    BackendMessage.Parameter.ApplicationName,
    BackendMessage.Parameter.IsSuperUser,
    BackendMessage.Parameter.SessionAuthorization,
    BackendMessage.Parameter.DateStyle,
    BackendMessage.Parameter.IntervalStyle,
    BackendMessage.Parameter.TimeZone,
    BackendMessage.Parameter.IntegerDateTimes,
    BackendMessage.Parameter.StandardConformingStrings,
    BackendMessage.Parameter.Other("other_param"),
  )
  implicit lazy val arbParam: Arbitrary[BackendMessage.ParameterStatus] = Arbitrary {
    for {
      param <- genParameter
      value <- Gen.alphaLowerStr.suchThat(_.nonEmpty)
    } yield BackendMessage.ParameterStatus(param, value)
  }
  implicit lazy val arbBackendKeyData: Arbitrary[BackendMessage.BackendKeyData] = Arbitrary {
    for {
      pid <- Arbitrary.arbitrary[Int]
      key <- Arbitrary.arbitrary[Int]
    } yield BackendMessage.BackendKeyData(pid, key)
  }

  implicit lazy val arbFieldDescription: Arbitrary[FieldDescription] = Arbitrary {
    for {
      name <- Gen.alphaStr.suchThat(_.nonEmpty)
      dataType <- Arbitrary.arbitrary[Oid]
      dataTypeSize <- Gen.oneOf(1, 2, 4, 8, 16).map(_.toShort)
      format <- Gen.oneOf(Format.Text, Format.Binary)
    } yield FieldDescription(name, None, None, dataType, dataTypeSize, 0, format)
  }

  implicit lazy val arbNamed: Arbitrary[Name.Named] = Arbitrary(Gen.alphaLowerStr.suchThat(_.nonEmpty).map(Name.Named))
  implicit lazy val arbName: Arbitrary[Name] =
    Arbitrary(Gen.oneOf(Gen.const(Name.Unnamed), Arbitrary.arbitrary[Name.Named]))

  implicit lazy val arbRowDescription: Arbitrary[RowDescription] = Arbitrary {
    Gen.nonEmptyListOf(arbFieldDescription.arbitrary).map(l => RowDescription(l.toIndexedSeq))
  }

  lazy val genBuf: Gen[Buf] = Arbitrary.arbitrary[Array[Byte]].map(bytes => Buf.ByteArray.Owned(bytes))
  implicit lazy val arbBuf: Arbitrary[Buf] = Arbitrary(genBuf)

  // TODO: this will need to be dervied from the dataType when used in a DataRow
  lazy val genValue: Gen[WireValue] = arbBuf.arbitrary.map(b => WireValue.Value(b))

  implicit lazy val arbValue: Arbitrary[WireValue] = Arbitrary {
    // TODO: more weight on non-null
    Gen.oneOf(Gen.const(WireValue.Null), genValue)
  }

  // TODO: produce the appropriate bytes based on the field descriptors. Should also include nulls.
  def genRowData(rowDescription: RowDescription): Gen[DataRow] =
    Gen.containerOfN[IndexedSeq, WireValue](rowDescription.rowFields.size, arbValue.arbitrary)
      .map(DataRow)

  lazy val genDataRow: Gen[DataRow] = for {
    row <- Arbitrary.arbitrary[RowDescription]
    data <- genRowData(row)
  } yield data

  implicit lazy val arbDataRow: Arbitrary[DataRow] = Arbitrary(genDataRow)

  // A self-contained, valid result set, i.e.: the row field data match the field descriptors
  case class TestResultSet(desc: RowDescription, rows: List[DataRow])
  implicit lazy val arbTestResultSet: Arbitrary[TestResultSet] = Arbitrary {
    for {
      desc <- arbRowDescription.arbitrary
      rows <- Gen.listOf(genRowData(desc))
    } yield TestResultSet(desc, rows)
  }

  lazy val genField: Gen[Field] = Gen.oneOf(
    Field.Code,
    Field.Column,
    Field.Constraint,
    Field.DataType,
    Field.Detail,
    Field.File,
    Field.Hint,
    Field.InternalPosition,
    Field.InternalQuery,
    Field.Line,
    Field.LocalizedSeverity,
    Field.Message,
    Field.Position,
    Field.Routine,
    Field.Schema,
    Field.Severity,
    Field.Table,
    Field.Where,
    Field.Unknown('U') // TODO
  )

  lazy val fieldMap: Gen[Map[Field, String]] = for {
    nbValues <- Gen.chooseNum(0, 8)
    keys <- Gen.containerOfN[List, Field](nbValues, genField)
    values <- Gen.containerOfN[List, String](nbValues, genAsciiString.map(_.value))
  } yield (keys zip values).toMap

  implicit lazy val arbErrorResponse: Arbitrary[BackendMessage.ErrorResponse] =
    Arbitrary(fieldMap.map(BackendMessage.ErrorResponse))
  implicit lazy val arbNoticeResponse: Arbitrary[BackendMessage.NoticeResponse] =
    Arbitrary(fieldMap.map(BackendMessage.NoticeResponse))

  implicit lazy val arbFormat: Arbitrary[Format] =
    Arbitrary(Gen.oneOf(Format.Text, Format.Binary))

  /**
   * Diffable[Buf] so we can `buf must_=== anotherBuf`
   */
  implicit lazy val bufDiffable: Diffable[Buf] = new Diffable[Buf] {
    override def diff(actual: Buf, expected: Buf): ComparisonResult = {
      val acArr = Buf.ByteArray.Shared.extract(actual)
      val exArr = Buf.ByteArray.Shared.extract(expected)
      new ComparisonResult {
        def hex(arr: Array[Byte]): String = {
          val h = arr.map(s => f"$s%02X").mkString
          s"0x$h"
        }

        override def identical: Boolean = java.util.Arrays.equals(acArr, exArr)

        override def render: String = s"${hex(acArr)} != ${hex(exArr)}"
      }
    }
  }

  lazy val genArrayDim: Gen[PgArrayDim] = Gen.chooseNum(1, 100).map { size =>
    PgArrayDim(size, 1)
  }
  lazy val genArray: Gen[PgArray] =
    for {
      dimensions <- Gen.chooseNum(1, 4)
      oid <- arbOid.arbitrary
      dims <- Gen.containerOfN[IndexedSeq, PgArrayDim](dimensions, genArrayDim)
      data <- Gen.containerOfN[IndexedSeq, WireValue](dims.map(_.size).sum, genValue)
    } yield PgArray(
      dimensions = dimensions,
      dataOffset = 0,
      elemType = oid,
      arrayDims = dims,
      data = data,
    )

  implicit lazy val arbPgArray: Arbitrary[PgArray] = Arbitrary(genArray)

  lazy val genInstant: Gen[Instant] = for {
    secs <- Gen.chooseNum(PgTime.Min.getEpochSecond, PgTime.Max.getEpochSecond)
    nanos <- Gen.chooseNum(PgTime.Min.getNano, PgTime.Max.getNano)
  } yield Instant.ofEpochSecond(secs, nanos.toLong).truncatedTo(ChronoUnit.MICROS)
  implicit val arbInstant: Arbitrary[Instant] = Arbitrary(genInstant)

  lazy val genMicros: Gen[Timestamp.Micros] =
    genInstant.map(i => i.getEpochSecond * 1000000 + i.getNano / 1000).map(Timestamp.Micros)
  lazy val genTimestamp: Gen[Timestamp] =
    Gen.frequency(99 -> genMicros, 1 -> Gen.oneOf(Timestamp.NegInfinity, Timestamp.Infinity))
  implicit lazy val arbTimestamp = Arbitrary(genTimestamp)

  lazy val genNumeric: Gen[Numeric] = implicitly[Arbitrary[BigDecimal]].arbitrary.map(PgNumeric.bigDecimalToNumeric)
  implicit lazy val arbNumeric: Arbitrary[Numeric] = Arbitrary(genNumeric)

}
