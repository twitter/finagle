package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.NoOp
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.finagle.postgresql.machine.StateMachine.TransitionResult
import com.twitter.io.Buf
import com.twitter.io.Pipe
import com.twitter.util.Future
import com.twitter.util.Return

/**
 * Unfortunately, postgresql has a different behaviour for this, it does not eagerly respond to individual request,
 * but expects that you simply send them in sequence or issue a [[Flush]] to get the response "synchronously".
 */
class ExtendedQueryMachine(name: Name, statement: String) extends StateMachine[Response.QueryResponse] {

  sealed trait State
  case object Parsing extends State
  case object Binding extends State
  case object Describing extends State
  case class Executing(r: RowDescription) extends State
  case class StreamResult(rowDescription: RowDescription, pipe: Pipe[DataRow], lastWrite: Future[Unit]) extends State {
    def append(row: DataRow): StreamResult =
      StreamResult(rowDescription, pipe, lastWrite before pipe.write(row))
    def resultSet: ResultSet = ResultSet(rowDescription, pipe)
  }
  case object Syncing extends State

  override def start: TransitionResult[State, Response.QueryResponse] =
    Transition(Parsing, Send(FrontendMessage.Parse(name, statement, Nil), flush = true))

  override def receive(state: State, msg: BackendMessage): TransitionResult[State, Response.QueryResponse] = (state, msg) match {
    case (Parsing, BackendMessage.ParseComplete) =>
      Transition(Binding, Send(FrontendMessage.Bind(Name.Unnamed, name, Nil, Nil, Nil), flush = true))

    case (Binding, BackendMessage.BindComplete) =>
      Transition(Describing, Send(FrontendMessage.Describe(Name.Unnamed, FrontendMessage.DescriptionTarget.Portal), flush = true))

    case (Describing, r: BackendMessage.RowDescription) =>
      Transition(Executing(r), Send(FrontendMessage.Execute(Name.Unnamed, 0), flush = true))

    case (Executing(r), row: DataRow) =>
      val stream = StreamResult(r, new Pipe, Future.Done).append(row)
      Transition(stream, Respond(Return(stream.resultSet)))

    case (r: StreamResult, dr: DataRow) => Transition(r.append(dr), NoOp)
    case (r: StreamResult, _: CommandComplete) =>
      // TODO: handle discard() to client can cancel the stream
      r.lastWrite.liftToTry.unit before r.pipe.close()
      Transition(Syncing, Send(FrontendMessage.Sync))
    case (Syncing, r: ReadyForQuery) => Complete(r, None)

    // TODO: error handling

    case (state, msg) => throw PgSqlNoSuchTransition("ExtendedQueryMachine", state, msg)
  }

}
