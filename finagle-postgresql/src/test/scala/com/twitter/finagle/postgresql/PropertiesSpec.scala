package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.specs2.ScalaCheck

trait PropertiesSpec extends ScalaCheck {

  // TODO: Once we have actual data types, Gen.oneOf(...)
  implicit lazy val arbOid = Arbitrary(Arbitrary.arbitrary[Int].map(Oid))

  implicit lazy val arbParam : Arbitrary[BackendMessage.ParameterStatus] = Arbitrary {
    for {
      name <- Gen.alphaLowerStr.suchThat(_.nonEmpty)
      value <- Gen.alphaLowerStr.suchThat(_.nonEmpty)
    } yield BackendMessage.ParameterStatus(name, value)
  }
  implicit lazy val arbBackendKeyData : Arbitrary[BackendMessage.BackendKeyData] = Arbitrary {
    for {
      pid <- Arbitrary.arbitrary[Int]
      key <- Arbitrary.arbitrary[Int]
    } yield BackendMessage.BackendKeyData(pid, key)
  }

  implicit lazy val arbFieldDescription: Arbitrary[FieldDescription] = Arbitrary {
    for {
      name <- Gen.alphaStr.suchThat(_.nonEmpty)
      dataType <- Arbitrary.arbitrary[Oid]
      dataTypeSize <- Gen.oneOf(1,2,4,8,16).map(_.toShort)
      format <- Gen.oneOf(Format.Text, Format.Binary)
    } yield FieldDescription(name, None, None, dataType, dataTypeSize, 0, format)
  }

  implicit lazy val arbNamed : Arbitrary[Name.Named] = Arbitrary(Gen.alphaLowerStr.suchThat(_.nonEmpty).map(Name.Named))
  implicit lazy val arbName : Arbitrary[Name] =
    Arbitrary(Gen.oneOf(Gen.const(Name.Unnamed), Arbitrary.arbitrary[Name.Named]))

  implicit lazy val arbRowDescription: Arbitrary[RowDescription] = Arbitrary {
    Gen.nonEmptyListOf(arbFieldDescription.arbitrary).map(l => RowDescription(l.toIndexedSeq))
  }

  implicit lazy val arbBuf: Arbitrary[Buf] =
    Arbitrary(Arbitrary.arbitrary[Array[Byte]].map { bytes => Buf.ByteArray.Owned(bytes) })

  // TODO: this will need to be dervied from the dataType when used in a DataRow
  val genValue = arbBuf.arbitrary.map(b => WireValue.Value(b))

  implicit lazy val arbValue: Arbitrary[WireValue] = Arbitrary {
    // TODO: more weight on non-null
    Gen.oneOf(Gen.const(WireValue.Null), genValue)
  }

  // TODO: produce the appropriate bytes based on the field descriptors. Should also include nulls.
  def arbDataRow(rowDescription: RowDescription): Arbitrary[DataRow] = Arbitrary {
    Gen.containerOfN[IndexedSeq, WireValue](rowDescription.rowFields.size, arbValue.arbitrary)
      .map(DataRow)
  }

  // A self-contained, valid result set, i.e.: the row field data match the field descriptors
  case class TestResultSet(desc: RowDescription, rows: List[DataRow])
  implicit lazy val arbTestResultSet: Arbitrary[TestResultSet] = Arbitrary {
    for {
      desc <- arbRowDescription.arbitrary
      rows <- Gen.listOf(arbDataRow(desc).arbitrary)
    } yield TestResultSet(desc, rows)
  }

  // TODO
  implicit lazy val arbErrorResponse: Arbitrary[BackendMessage.ErrorResponse] =
    Arbitrary(Gen.const(BackendMessage.ErrorResponse(Map.empty)))

}
