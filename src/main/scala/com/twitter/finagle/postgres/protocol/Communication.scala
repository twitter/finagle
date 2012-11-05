package com.twitter.finagle.postgres.protocol

import java.sql.{Date => SQLDate}
import java.sql.Timestamp

import com.twitter.logging.Logger


case class PgRequest(msg: FrontendMessage) {

}

trait PgResponse {
}

case class SingleMessageResponse(msg: BackendMessage) extends PgResponse {
}

case class Error(msg: Option[String]) extends PgResponse {
}

sealed trait PasswordEncoding

object ClearText extends PasswordEncoding

case class Md5(salt: Array[Byte]) extends PasswordEncoding

case class PasswordRequired(encoding: PasswordEncoding) extends PgResponse

case class AuthenticatedResponse(params: Map[String, String], processId: Int, secretKey: Int) extends PgResponse {
}

sealed trait QueryResponse extends PgResponse
sealed trait Value {
}

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

// TODO any sense of having Field instead of string???
case class Field(name: String)


class Row(val fields: IndexedSeq[Field], val vals: IndexedSeq[Value]) {
  private val logger = Logger(getClass.getName)

  private[this] val indexMap = fields.map(_.name).zipWithIndex.toMap

  def get(name: String): Option[Value] = {
    indexMap.get(name).map(vals(_))
  }

  def getString(name: String): String = {
    val value = get(name) map {
      case StringValue(s) => s
      case _ => throw new IllegalStateException("Expected string value")
    }
    value.get
  }

  def get(index: Int): Value = vals(index)

  def values(): IndexedSeq[Value] = vals

  override def toString(): String = "{ fields='" + fields.toString + "', rows='" + vals.toString + "'"

}

case class ResultSet(fields: IndexedSeq[Field], rows: Seq[Row]) extends QueryResponse

case class Inserted(num: Int) extends QueryResponse

case class Deleted(num: Int) extends QueryResponse

case class Updated(num: Int) extends QueryResponse

object Communication {

  def request(msg: FrontendMessage) = new PgRequest(msg)

}