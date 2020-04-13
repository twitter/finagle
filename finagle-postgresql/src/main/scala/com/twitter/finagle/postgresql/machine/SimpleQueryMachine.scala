package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.NoticeResponse
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.PgSqlStateMachineError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

class SimpleQueryMachine(query: String) extends StateMachine[Response] {

  sealed trait State
  case object Init extends State
  case class BuildResult(rowDescription: RowDescription, rows: List[DataRow]) extends State {
    def append(row: DataRow): BuildResult = copy(rows = row :: rows)
    def resultSet: ResultSet = ResultSet(rowDescription, rows.reverse)
  }
  case class Complete(response: Try[Response]) extends State

  override def start: StateMachine.TransitionResult[State, Response] =
    StateMachine.TransitionAndSend(Init, FrontendMessage.Query(query))

  override def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, Response] = (state, msg) match {
    case (Init, EmptyQueryResponse) => StateMachine.Transition(Complete(Return(Response.BackendResponse(EmptyQueryResponse))))
    case (Init, c: CommandComplete) => StateMachine.Transition(Complete(Return(Response.BackendResponse(c))))

    case (Init, rd: RowDescription) => StateMachine.Transition(BuildResult(rd, Nil))
    case (r: BuildResult, dr: DataRow) => StateMachine.Transition(r.append(dr))
    case (r: BuildResult, c: CommandComplete) => StateMachine.Transition(Complete(Return(r.resultSet)))

    case (Complete(response), r: ReadyForQuery) => StateMachine.Respond(response, Future.value(r))
    case (state, _: NoticeResponse) => StateMachine.Transition(state) // TODO: don't ignore
    case (_, e: ErrorResponse) => StateMachine.Transition(Complete(Throw(PgSqlServerError(e))))
    case (state, msg) => throw PgSqlStateMachineError("SimpleQueryMachine", state, msg)
  }
}
