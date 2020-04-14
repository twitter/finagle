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
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.io.Pipe
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw

class SimpleQueryMachine(query: String) extends StateMachine[Response] {

  import StateMachine._

  sealed trait State
  case object Init extends State
  case class StreamResult(rowDescription: RowDescription, pipe: Pipe[DataRow], lastWrite: Future[Unit]) extends State {
    def append(row: DataRow): StreamResult =
      StreamResult(rowDescription, pipe, lastWrite before pipe.write(row))
    def resultSet: ResultSet = ResultSet(rowDescription, pipe)
  }
  case object ExpectReady extends State

  override def start: StateMachine.TransitionResult[State, Response] =
    StateMachine.Transition(Init, StateMachine.Send(FrontendMessage.Query(query)))

  override def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, Response] = (state, msg) match {
    case (Init, EmptyQueryResponse) =>
      Transition(ExpectReady, Respond(Return(BackendResponse(EmptyQueryResponse))))
    case (Init, c: CommandComplete) =>
      Transition(ExpectReady, Respond(Return(BackendResponse(c))))

    case (Init, rd: RowDescription) =>
      val state = StreamResult(rd, new Pipe, Future.Done)
      Transition(state, Respond(Return(state.resultSet)))
    case (r: StreamResult, dr: DataRow) => Transition(r.append(dr), NoOp)
    case (r: StreamResult, _: CommandComplete) =>
      // TODO: handle discard() to client can cancel the stream
      r.lastWrite.liftToTry.unit before r.pipe.close()
      Transition(ExpectReady, NoOp)

    case (ExpectReady, r: ReadyForQuery) => Complete(r, None)

    case (state, _: NoticeResponse) => Transition(state, NoOp) // TODO: don't ignore
    case (_, e: ErrorResponse) => Transition(ExpectReady, Respond(Throw(PgSqlServerError(e))))
    case (state, msg) => throw PgSqlNoSuchTransition("SimpleQueryMachine", state, msg)
  }
}
