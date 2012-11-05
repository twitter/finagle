package com.twitter.finagle.postgres.protocol

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

case class MessageSequenceResponse(messages: List[BackendMessage]) extends PgResponse {
}

object Communication {

  def request(msg: FrontendMessage) = new PgRequest(msg)

  def response(msg: BackendMessage) = new SingleMessageResponse(msg)

  def sequence(messages: List[BackendMessage]) = new MessageSequenceResponse(messages)

}