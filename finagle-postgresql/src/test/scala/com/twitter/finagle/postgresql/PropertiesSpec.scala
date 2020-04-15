package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.FieldDescription
import com.twitter.finagle.postgresql.BackendMessage.Format
import com.twitter.finagle.postgresql.BackendMessage.Oid
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.specs2.ScalaCheck

trait PropertiesSpec extends ScalaCheck {

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
      dataType <- Arbitrary.arbitrary[Int].map(Oid) // TODO: Once we have actual data types, Gen.oneOf(...)
      dataTypeSize <- Gen.oneOf(1,2,4,8,16).map(_.toShort)
      format <- Gen.oneOf(Format.Text, Format.Binary)
    } yield FieldDescription(name, None, None, dataType, dataTypeSize, 0, format)
  }
  implicit lazy val arbRowDescription: Arbitrary[RowDescription] = Arbitrary {
    Gen.nonEmptyListOf(arbFieldDescription.arbitrary).map(l => RowDescription(l.toIndexedSeq))
  }

  // TODO: this will need to be dervied from the dataType when used in a DataRow
  implicit lazy val arbBuf: Arbitrary[Buf] =
    Arbitrary(Arbitrary.arbitrary[Array[Byte]].map { bytes => Buf.ByteArray.Owned(bytes) })

  // TODO: produce the appropriate bytes based on the field descriptors. Should also include nulls.
  def arbDataRow(rowDescription: RowDescription): Arbitrary[DataRow] = Arbitrary {
    Gen.containerOfN[IndexedSeq, Buf](rowDescription.rowFields.size, arbBuf.arbitrary)
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
