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
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.finagle.postgresql.Response.SimpleQueryResponse
import com.twitter.io.Pipe
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw

class SimpleQueryMachine(query: String) extends StateMachine[SimpleQueryResponse] {

  import StateMachine._

  sealed trait State
  case object Sent extends State
  case class StreamResponses(pipe: Pipe[Response.QueryResponse], lastWrite: Future[Unit]) extends State {
    def append(response: Response.QueryResponse): StreamResponses =
      StreamResponses(pipe, lastWrite before pipe.write(response))
  }
  object StreamResponses {
    def init = StreamResponses(new Pipe, Future.Done)
  }
  case class StreamResult(responses: StreamResponses, rowDescription: RowDescription, pipe: Pipe[DataRow], lastWrite: Future[Unit]) extends State {
    def append(row: DataRow): StreamResult =
      StreamResult(responses, rowDescription, pipe, lastWrite before pipe.write(row))
    def resultSet: ResultSet = ResultSet(rowDescription, pipe)
  }

  override def start: StateMachine.TransitionResult[State, SimpleQueryResponse] =
    StateMachine.Transition(Sent, StateMachine.Send(FrontendMessage.Query(query)))

  def handleResponse(responses: StreamResponses, msg: BackendMessage): State = msg match {
    case EmptyQueryResponse => responses.append(Response.Empty)
    case CommandComplete(commandTag) => responses.append(Response.Command(commandTag))
    case rd: RowDescription =>
      val state = StreamResult(responses, rd, new Pipe, Future.Done)
      responses.append(state.resultSet)
      state
    case _ => sys.error("") // TODO
  }

  override def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, SimpleQueryResponse] = (state, msg) match {
    case (Sent, EmptyQueryResponse | _: CommandComplete | _: RowDescription) =>
      val response = StreamResponses.init
      Transition(handleResponse(response, msg), Respond(Return(Response.SimpleQueryResponse(response.pipe))))
    case (s: StreamResponses, EmptyQueryResponse | _: CommandComplete | _: RowDescription) =>
      Transition(handleResponse(s, msg), NoOp)

    case (r: StreamResult, dr: DataRow) => Transition(r.append(dr), NoOp)
    case (r: StreamResult, _: CommandComplete) =>
      // TODO: handle discard() to client can cancel the stream
      r.lastWrite.liftToTry.unit before r.pipe.close()
      Transition(r.responses, NoOp)
    case (r: StreamResult, e: ErrorResponse) =>
      val exception = PgSqlServerError(e)
      r.pipe.fail(exception)
      // We've already responded at this point, so this will likely not do anything.
      Transition(r.responses, Respond(Throw(exception)))

    case (s: StreamResponses, r: ReadyForQuery) =>
      s.lastWrite.liftToTry.unit before s.pipe.close()
      Complete(r, None)

    case (s: StreamResponses, e: ErrorResponse) =>
      val exception = PgSqlServerError(e)
      s.pipe.fail(exception)
      // We've already responded at this point, so this will likely not do anything.
      Transition(s, Respond(Throw(exception)))

    case (state, _: NoticeResponse) => Transition(state, NoOp) // TODO: don't ignore

    case (Sent, e: ErrorResponse) => Transition(Sent, Respond(Throw(PgSqlServerError(e))))
    case (Sent, r:ReadyForQuery) => Complete(r, None)

    case (state, msg) => throw PgSqlNoSuchTransition("SimpleQueryMachine", state, msg)
  }
}
