package com.twitter.finagle.postgres.protocol

import java.sql.{Date => SQLDate}
import java.sql.Timestamp

case class PgRequest(msg: FrontendMessage, flush: Boolean)

trait PgResponse

case class SingleMessageResponse(msg: BackendMessage) extends PgResponse

case class Error(msg: Option[String]) extends PgResponse

case object ParseCompletedResponse extends PgResponse

case object BindCompletedResponse extends PgResponse

case object ReadyForQueryResponse extends PgResponse

sealed trait PasswordEncoding

object ClearText extends PasswordEncoding

case class Md5(salt: Array[Byte]) extends PasswordEncoding

case class PasswordRequired(encoding: PasswordEncoding) extends PgResponse

case class AuthenticatedResponse(params: Map[String, String], processId: Int, secretKey: Int) extends PgResponse

sealed trait Value

case class StringValue(s: String) extends Value

case class BooleanValue(b: Boolean) extends Value

case class ByteValue(b: Byte) extends Value

case class ShortValue(s: Short) extends Value

case class IntValue(i: Int) extends Value

case class LongValue(l: Long) extends Value

case class FloatValue(f: Float) extends Value

case class DoubleValue(d: Double) extends Value

case class TimestampValue(t: Timestamp) extends Value

case class DateValue(d: SQLDate) extends Value

case object NullValue extends Value

case class Rows(rows: List[DataRow], completed: Boolean) extends PgResponse

case class Field(name: String, format: Int, dataType: Int)

case class RowDescriptions(fields: IndexedSeq[Field]) extends PgResponse

case class Descriptions(params: IndexedSeq[Int], fields: IndexedSeq[Field]) extends PgResponse

case class ParamsResponse(types: IndexedSeq[Int]) extends PgResponse

case class SelectResult(fields: IndexedSeq[Field], rows: List[DataRow]) extends PgResponse

case class CommandCompleteResponse(affectedRows: Int) extends PgResponse

object Communication {

  def request(msg: FrontendMessage, flush: Boolean = false) = new PgRequest(msg, flush)

}