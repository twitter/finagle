package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.BindComplete
import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.NoData
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.FrontendMessage.Bind
import com.twitter.finagle.postgresql.FrontendMessage.Describe
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.FrontendMessage.Execute
import com.twitter.finagle.postgresql.FrontendMessage.Flush
import com.twitter.finagle.postgresql.FrontendMessage.Sync
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.NoOp
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.SendSeveral
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.finagle.postgresql.machine.StateMachine.TransitionResult
import com.twitter.io.Buf
import com.twitter.io.Pipe
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

/**
 * Unfortunately, postgresql has a different behaviour for this, it does not eagerly respond to individual request,
 * but expects that you simply send them in sequence or issue a [[Flush]] to get the response "synchronously".
 *
 * For this reason, this machine issues several messages on startup and expects the responses to happen in order.
 * It's not clear if the backend is allowed to send them in a different order.
 */
class ExtendedQueryMachine(name: Name, parameters: IndexedSeq[Buf]) extends StateMachine[Response.QueryResponse] {

  sealed trait State
  case object Binding extends State
  case object Describing extends State
  case object ExecutingCommand extends State
  case class Executing(r: RowDescription) extends State
  case class StreamResult(rowDescription: RowDescription, pipe: Pipe[DataRow], lastWrite: Future[Unit]) extends State {
    def append(row: DataRow): StreamResult =
      StreamResult(rowDescription, pipe, lastWrite before pipe.write(row))
    def resultSet: ResultSet = ResultSet(rowDescription.rowFields, pipe)
  }
  case class Syncing(response: Option[Try[Response.QueryResponse]]) extends State

  override def start: TransitionResult[State, Response.QueryResponse] =
    Transition(
      Binding,
      SendSeveral(
        Bind(Name.Unnamed, name, Nil, Nil, Nil), // TODO: deal with parameters
        Describe(Name.Unnamed, DescriptionTarget.Portal), // TODO: we can avoid this one when one from Prepared returned NoData
        Execute(Name.Unnamed, 0), // TODO: allow portal suspension
        Flush
      )
    )

  override def receive(state: State, msg: BackendMessage): TransitionResult[State, Response.QueryResponse] = (state, msg) match {
    case (Binding, BindComplete) =>
      Transition(Describing, NoOp)

    case (Describing, NoData) =>
      Transition(ExecutingCommand, NoOp)
    case (ExecutingCommand, CommandComplete(tag)) =>
      Transition(Syncing(Some(Return(Response.Command(tag)))), Send(Sync))

    case (Describing, r: RowDescription) =>
      Transition(Executing(r), NoOp)

    case (Executing(_), EmptyQueryResponse) =>
      Transition(Syncing(Some(Return(Response.Empty))), Send(Sync))
    case (Executing(r), row: DataRow) =>
      val stream = StreamResult(r, new Pipe, Future.Done).append(row)
      Transition(stream, Respond(Return(stream.resultSet)))

    case (r: StreamResult, dr: DataRow) => Transition(r.append(dr), NoOp)
    case (r: StreamResult, _: CommandComplete) =>
      // TODO: handle discard() to client can cancel the stream
      r.lastWrite.liftToTry.unit before r.pipe.close()
      Transition(Syncing(None), Send(Sync))
    case (r: StreamResult, e: ErrorResponse) =>
      val exception = PgSqlServerError(e)
      r.pipe.fail(exception)
      Transition(Syncing(Some(Throw(exception))), Send(Sync))

    case (Syncing(response), r: ReadyForQuery) => Complete(r, response)

    case (_, e: ErrorResponse) =>
      Transition(Syncing(Some(Throw(PgSqlServerError(e)))), Send(Sync))

    case (state, msg) => throw PgSqlNoSuchTransition("ExtendedQueryMachine", state, msg)
  }

}
