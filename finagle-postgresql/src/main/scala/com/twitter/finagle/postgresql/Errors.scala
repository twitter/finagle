package com.twitter.finagle.postgresql

abstract class PgSqlException(message: String) extends RuntimeException(message)
case class PgSqlServerError(error: BackendMessage.ErrorResponse)
  extends PgSqlException(error.values.getOrElse(BackendMessage.Field.Message, "Server error")) {
  def field(f: BackendMessage.Field): Option[String] = error.values.get(f)
}
case object PgSqlTlsUnsupportedError extends PgSqlException("TLS is not supported by the server.")

class PgSqlClientError(message: String) extends PgSqlException(message)
case class PgSqlUnsupportedError(msg: String) extends PgSqlClientError(msg)
case object PgSqlPasswordRequired extends PgSqlClientError("Password was not provided but is required.")
case class PgSqlUnsupportedAuthenticationMechanism(method: BackendMessage.AuthenticationMessage)
  extends PgSqlClientError(s"Unsupported authentication mechanism.")

sealed abstract class PgSqlStateMachineError(message: String) extends PgSqlClientError(message)
case class PgSqlInvalidMachineStateError(msg: String) extends PgSqlStateMachineError(msg)
case class PgSqlNoSuchTransition(machine: String, state: String, msg: String)
  extends PgSqlStateMachineError(s"State machine $machine in state $state has no transition defined for message $msg")
object PgSqlNoSuchTransition {
  def apply[S, B <: BackendMessage](machine: String, state: S, msg: B): PgSqlNoSuchTransition =
    PgSqlNoSuchTransition(machine, state.toString, msg.toString) // TODO: Show or similar instead of toString
}
