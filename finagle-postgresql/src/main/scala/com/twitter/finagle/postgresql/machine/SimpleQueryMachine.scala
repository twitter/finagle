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
import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.io.Pipe
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw

class SimpleQueryMachine(query: String) extends StateMachine[Response] {

  sealed trait State
  case object Init extends State
  case class StreamResult(rowDescription: RowDescription, pipe: Pipe[DataRow], lastWrite: Future[Unit]) extends State {
    def append(row: DataRow): StreamResult =
      StreamResult(rowDescription, pipe, lastWrite before pipe.write(row))
    def resultSet: ResultSet = ResultSet(rowDescription, pipe)
  }
  case object Complete extends State

  override def start: StateMachine.TransitionResult[State, Response] =
    StateMachine.TransitionAndSend(Init, FrontendMessage.Query(query))

  override def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, Response] = (state, msg) match {
    case (Init, EmptyQueryResponse) => StateMachine.Respond(Complete, Return(BackendResponse(EmptyQueryResponse)))
    case (Init, c: CommandComplete) => StateMachine.Respond(Complete, Return(BackendResponse(c)))

    case (Init, rd: RowDescription) =>
      val state = StreamResult(rd, new Pipe, Future.Done)
      StateMachine.Respond(state, Return(state.resultSet))
    case (r: StreamResult, dr: DataRow) => StateMachine.Transition(r.append(dr))
    case (r: StreamResult, _: CommandComplete) =>
      // TODO: handle discard() to client can cancel the stream
      r.lastWrite.liftToTry.unit before r.pipe.close()
      StateMachine.Transition(Complete)

    case (Complete, r: ReadyForQuery) => StateMachine.Complete(r, None)

    case (state, _: NoticeResponse) => StateMachine.Transition(state) // TODO: don't ignore
    case (_, e: ErrorResponse) => StateMachine.Respond(Complete, Throw(PgSqlServerError(e)))
    case (state, msg) => throw PgSqlStateMachineError("SimpleQueryMachine", state, msg)
  }
}
