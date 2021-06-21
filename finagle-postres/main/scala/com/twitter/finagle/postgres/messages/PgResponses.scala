package com.twitter.finagle.postgres.messages

import com.twitter.concurrent.AsyncStream
import com.twitter.util.Future

/*
 * Response message types.
 */
trait PgResponse

trait AsyncPgResponse extends PgResponse {
  // this is fulfilled when the connection has finished processing the previous request and is ready for new ones.
  private[finagle] val complete: Future[Unit]
}

case class SingleMessageResponse(msg: BackendMessage) extends PgResponse

case class Error(msg: Option[String], severity: Option[String] = None, sqlState: Option[String] = None, detail: Option[String] = None, hint: Option[String] = None, position: Option[String] = None) extends PgResponse

object Error {
  def apply(params: Map[Char,String]): Error =
    Error(params.get('M'), params.get('S'), params.get('C'), params.get('D'), params.get('H'), params.get('P'))
}

case object SslSupportedResponse extends PgResponse

case object SslNotSupportedResponse extends PgResponse

case object ParseCompletedResponse extends PgResponse

case object BindCompletedResponse extends PgResponse

case object ReadyForQueryResponse extends PgResponse

sealed trait PasswordEncoding

object ClearText extends PasswordEncoding

case class Md5(salt: Array[Byte]) extends PasswordEncoding

case class PasswordRequired(encoding: PasswordEncoding) extends PgResponse

case class AuthenticatedResponse(params: Map[String, String], processId: Int, secretKey: Int) extends PgResponse

case class Rows(rows: AsyncStream[DataRow])(private[finagle] val complete: Future[Unit]) extends AsyncPgResponse
object Rows {
  val Empty = Rows(AsyncStream.empty)(Future.Done)
}

case class Field(name: String, format: Short, dataType: Int)

case class RowDescriptions(fields: Array[Field]) extends PgResponse

case class Descriptions(params: Array[Int], fields: Array[Field]) extends PgResponse

case class ParamsResponse(types: Array[Int]) extends PgResponse

case class SelectResult(fields: Array[Field], rows: AsyncStream[DataRow])(private[finagle] val complete: Future[Unit]) extends AsyncPgResponse
object SelectResult {
  val Empty = SelectResult(Array.empty, AsyncStream.empty)(Future.Done)
}

case class CommandCompleteResponse(affectedRows: Int) extends PgResponse

case object Terminated extends PgResponse
