package com.twitter.finagle.postgresql

abstract class PgSqlException extends RuntimeException
case class PgSqlServerError(error: BackendMessage.ErrorResponse) extends PgSqlException {
  override def getMessage: String = field(BackendMessage.Field.Message).getOrElse("Server error")

  def field(f: BackendMessage.Field): Option[String] = error.values.get(f)
}
case object PgSqlTlsUnsupportedError extends PgSqlException

abstract class PgSqlClientError extends PgSqlException
case class PgSqlUnsupportedError(msg: String) extends PgSqlClientError {
  override def getMessage: String = msg
}
case object PgSqlPasswordRequired extends PgSqlClientError
case class PgSqlUnsupportedAuthenticationMechanism(method: BackendMessage.AuthenticationMessage) extends PgSqlClientError

sealed trait PgSqlStateMachineError extends PgSqlClientError
case class PgSqlInvalidMachineStateError(msg: String) extends PgSqlStateMachineError {
  override def getMessage: String = msg
}
case class PgSqlNoSuchTransition(machine: String, state: String, msg: String) extends PgSqlStateMachineError {
  override def getMessage: String = s"State machine $machine in state $state has no transition defined for message $msg"
}
object PgSqlNoSuchTransition {
  def apply[S, B <: BackendMessage](machine: String, state: S, msg: B): PgSqlNoSuchTransition =
    PgSqlNoSuchTransition(machine, state.toString, msg.toString) // TODO: Show or similar instead of toString
}
