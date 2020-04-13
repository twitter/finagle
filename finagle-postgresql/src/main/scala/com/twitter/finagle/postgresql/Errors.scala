package com.twitter.finagle.postgresql

import scala.reflect.ClassTag

abstract class PgSqlException extends RuntimeException
case class PgSqlServerError(e: BackendMessage.ErrorResponse) extends PgSqlException {
  override def getMessage: String = PgSqlServerError.field(BackendMessage.Field.Message, e).getOrElse("Server error")
}
object PgSqlServerError {
  def field(f: BackendMessage.Field, e: BackendMessage.ErrorResponse): Option[String] = e.values.get(f)
}

abstract class PgSqlClientError extends PgSqlException
case class PgSqlStateMachineError(machine: String, state: String, msg: String) extends PgSqlClientError {
  override def getMessage: String = s"State machine $machine in state $state has no transition defined for message $msg"
}
object PgSqlStateMachineError {
  // uses reflection to get the class name. Uses ClassTag to do this safely (avoid https://github.com/scala/bug/issues/2034)
  def apply[B <: BackendMessage](machine: String, state: String, msg: B)(implicit ct: ClassTag[B]): PgSqlStateMachineError =
    PgSqlStateMachineError(machine, state, ct.toString)
}
