package com.twitter.finagle.postgresql

import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import com.twitter.finagle.postgresql.BackendMessage.CommandTag
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.Field
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Inet
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Numeric
import com.twitter.finagle.postgresql.Types.NumericSign
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.PgArray
import com.twitter.finagle.postgresql.Types.PgArrayDim
import com.twitter.finagle.postgresql.Types.Timestamp
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.types.Json
import com.twitter.finagle.postgresql.types.PgDate
import com.twitter.finagle.postgresql.types.PgNumeric
import com.twitter.finagle.postgresql.types.PgTime
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.Assertion
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import java.nio.charset.StandardCharsets
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait PropertiesSpec extends ScalaCheckPropertyChecks with org.scalatest.matchers.must.Matchers {

  private def mkJson(value: String): Json = {
    Json(Buf.Utf8(value), StandardCharsets.UTF_8)
  }

  implicit val arbitraryJson = Arbitrary(Gen.oneOf(Seq(mkJson("{\"a\": 10}"))))

  case class AsciiString(value: String)
  lazy val genAsciiChar: Gen[Char] = Gen.choose(32.toChar, 126.toChar)
  lazy val genAsciiString: Gen[AsciiString] =
    Gen.listOf(genAsciiChar).map(_.mkString).map(AsciiString)
  implicit lazy val arbAsciiString: Arbitrary[AsciiString] = Arbitrary(genAsciiString)

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
      value <- genNonEmptyLowerStr
    } yield BackendMessage.ParameterStatus(param, value)
  }
  implicit lazy val arbBackendKeyData: Arbitrary[BackendMessage.BackendKeyData] = Arbitrary {
    for {
      pid <- Arbitrary.arbitrary[Int]
      key <- Arbitrary.arbitrary[Int]
    } yield BackendMessage.BackendKeyData(pid, key)
  }

  val genCommandTag: Gen[CommandTag] =
    for {
      rows <- Gen.chooseNum(0, Int.MaxValue)
      cmd <- Gen.oneOf(
        CommandTag.Insert,
        CommandTag.Update,
        CommandTag.Delete,
        CommandTag.Select,
        CommandTag.Move,
        CommandTag.Fetch,
      )
      tag <- Gen.oneOf(
        CommandTag.AffectedRows(cmd, rows),
        CommandTag.Other("SOME TAG")
      )
    } yield tag
  implicit lazy val arbCommandTag: Arbitrary[CommandTag] = Arbitrary(genCommandTag)

  lazy val genNonEmptyStr = for { h <- Gen.alphaChar; t <- Gen.alphaStr } yield h + t
  implicit lazy val arbFieldDescription: Arbitrary[FieldDescription] = Arbitrary {
    for {
      name <- genNonEmptyStr
      dataType <- Arbitrary.arbitrary[Oid]
      dataTypeSize <- Gen.oneOf(1, 2, 4, 8, 16).map(_.toShort)
      format <- Gen.oneOf(Format.Text, Format.Binary)
    } yield FieldDescription(name, None, None, dataType, dataTypeSize, 0, format)
  }

  lazy val genNonEmptyLowerStr = for { h <- Gen.alphaLowerChar; t <- Gen.alphaLowerStr } yield h + t
  implicit lazy val arbNamed: Arbitrary[Name.Named] = Arbitrary(genNonEmptyLowerStr.map(Name.Named))
  implicit lazy val arbName: Arbitrary[Name] =
    Arbitrary(Gen.oneOf(Gen.const(Name.Unnamed), Arbitrary.arbitrary[Name.Named]))

  implicit lazy val arbRowDescription: Arbitrary[RowDescription] = Arbitrary {
    Gen.nonEmptyListOf(arbFieldDescription.arbitrary).map(l => RowDescription(l.toIndexedSeq))
  }

  lazy val genBuf: Gen[Buf] =
    Arbitrary.arbitrary[Array[Byte]].map(bytes => Buf.ByteArray.Owned(bytes))
  implicit lazy val arbBuf: Arbitrary[Buf] = Arbitrary(genBuf)

  // TODO: this will need to be dervied from the dataType when used in a DataRow
  lazy val genValue: Gen[WireValue] = arbBuf.arbitrary.map(b => WireValue.Value(b))

  implicit lazy val arbValue: Arbitrary[WireValue] = Arbitrary {
    // TODO: more weight on non-null
    Gen.oneOf(Gen.const(WireValue.Null), genValue)
  }

  // TODO: produce the appropriate bytes based on the field descriptors. Should also include nulls.
  def genRowData(rowDescription: RowDescription): Gen[DataRow] =
    Gen
      .containerOfN[IndexedSeq, WireValue](rowDescription.rowFields.size, arbValue.arbitrary)
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

  lazy val nonEmptyTestResultSet: Arbitrary[TestResultSet] = Arbitrary {
    for {
      desc <- arbRowDescription.arbitrary
      rows <- Gen.nonEmptyListOf(genRowData(desc))
    } yield TestResultSet(desc, rows)
  }

  implicit lazy val arbTarget: Arbitrary[DescriptionTarget] =
    Arbitrary(Gen.oneOf(DescriptionTarget.PreparedStatement, DescriptionTarget.Portal))

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

  lazy val genLocalDate: Gen[LocalDate] = for {
    days <- Gen.chooseNum(-2451545, 2145031948)
  } yield PgDate.Epoch.plusDays(days.toLong)
  implicit val arbLocalDate: Arbitrary[LocalDate] = Arbitrary(genLocalDate)

  lazy val genNumericSign: Gen[NumericSign] = Gen.oneOf(
    NumericSign.Positive,
    NumericSign.Negative,
    NumericSign.NaN,
    NumericSign.NegInfinity,
    NumericSign.Infinity
  )
  implicit lazy val arbNumericSign: Arbitrary[NumericSign] = Arbitrary(genNumericSign)
  lazy val genNumeric: Gen[Numeric] =
    implicitly[Arbitrary[BigDecimal]].arbitrary.map(PgNumeric.bigDecimalToNumeric)
  implicit lazy val arbNumeric: Arbitrary[Numeric] = Arbitrary(genNumeric)

  lazy val genIp: Gen[java.net.InetAddress] = for {
    size <- Gen.oneOf(4, 16)
    addr <- Gen.buildableOfN[Array[Byte], Byte](size, Arbitrary.arbitrary[Byte])
  } yield java.net.InetAddress.getByAddress(addr)
  implicit lazy val arbIp: Arbitrary[java.net.InetAddress] = Arbitrary(genIp)

  lazy val genInet: Gen[Inet] = for {
    ip <- genIp
    mask <- ip match {
      case _: java.net.Inet4Address => Gen.choose(0, 32)
      case _: java.net.Inet6Address => Gen.choose(0, 128)
    }
  } yield Inet(ip, mask.toShort)
  implicit lazy val arbInet: Arbitrary[Inet] = Arbitrary(genInet)

  implicit lazy val arbBackendCopyData: Arbitrary[BackendMessage.CopyData] =
    Arbitrary(genBuf.map(BackendMessage.CopyData))
  implicit lazy val arbFrontendCopyData: Arbitrary[FrontendMessage.CopyData] =
    Arbitrary(genBuf.map(FrontendMessage.CopyData))

  // ---------------
  // NOTE: these functions below are helpers to simplify the migration from specs2 to scalatest,
  // prefer built-in functions to these when writing new tests.
  def beLike[T](pattern: PartialFunction[T, Assertion]): Matcher[T] = new Matcher[T] {
    override def apply(left: T): MatchResult = {
      if (pattern.isDefinedAt(left)) pattern.apply(left) else fail()
      MatchResult(matches = true, "", "")
    }
  }

  def beSuccessfulTry[A, B](other: B): Matcher[scala.util.Try[A]] =
    beSuccessfulTry(new Matcher[A] {
      override def apply(left: A): MatchResult = {
        MatchResult(other == left.asInstanceOf[B], s"$other != $left", s"$other == $left")
      }
    })

  def beSuccessfulTry[A](other: Matcher[A]): Matcher[scala.util.Try[A]] = new Matcher[Try[A]] {
    override def apply(left: Try[A]): MatchResult = {
      left match {
        case Success(value) => other.apply(value)
        case f @ Failure(_) =>
          MatchResult(
            matches = false,
            s"Expected success but got $f",
            "Expected failure but got success")
      }
    }
  }

  def prop[A, ASSERTION](fn: A => ASSERTION)(implicit arbA: Arbitrary[A]): Unit = {
    forAll(fn)
  }

  def prop[A, B, ASSERTION](
    fn: (A, B) => ASSERTION
  )(
    implicit arbA: Arbitrary[A],
    arbB: Arbitrary[B]
  ): Unit = {
    forAll(fn)
  }

  def prop[A, B, C, ASSERTION](
    fn: (A, B, C) => ASSERTION
  )(
    implicit arbA: Arbitrary[A],
    arbB: Arbitrary[B],
    arbC: Arbitrary[C]
  ): Unit = {
    forAll(fn)
  }

  def prop[A, B, C, D, ASSERTION](
    fn: (A, B, C, D) => ASSERTION
  )(
    implicit arbA: Arbitrary[A],
    arbB: Arbitrary[B],
    arbC: Arbitrary[C],
    arbD: Arbitrary[D]
  ): Unit = {
    forAll(fn)
  }
}
