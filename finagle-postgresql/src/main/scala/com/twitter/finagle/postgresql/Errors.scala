package com.twitter.finagle.postgresql

abstract class PgSqlException extends RuntimeException
case class PgSqlServerError(error: BackendMessage.ErrorResponse) extends PgSqlException {
  override def getMessage: String = field(BackendMessage.Field.Message).getOrElse("Server error")

  def field(f: BackendMessage.Field): Option[String] = error.values.get(f)
}

abstract class PgSqlClientError extends PgSqlException
case class PgSqlStateMachineError(machine: String, state: String, msg: String) extends PgSqlClientError {
  override def getMessage: String = s"State machine $machine in state $state has no transition defined for message $msg"
}
object PgSqlStateMachineError {
  def apply[S, B <: BackendMessage](machine: String, state: S, msg: B): PgSqlStateMachineError =
    PgSqlStateMachineError(machine, state.toString, msg.toString) // TODO: Show or similar instead of toString
}
