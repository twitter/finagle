package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.BindComplete
import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.InTx
import com.twitter.finagle.postgresql.BackendMessage.NoData
import com.twitter.finagle.postgresql.BackendMessage.PortalSuspended
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
import com.twitter.finagle.postgresql.Request
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Response.ConnectionParameters
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.NoOp
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.SendSeveral
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.finagle.postgresql.machine.StateMachine.TransitionResult
import com.twitter.io.Pipe
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

/**
 * Implements part of the "Extended Query" message flow described here https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY.
 *
 * This machine is used in combination with [[PrepareMachine]]. That is, before executing this machine, a prior
 * execution of [[PrepareMachine]] must have taken place.
 *
 * NOTE: this machine is slightly different from other ones in that it will send multiple messages on start and then
 * a [[Flush]]. The reason is because the message flow is different in this case and the backend does not send
 * individual responses until the [[Flush]] is received. The machine expects responses to come back in order,
 * but it's not entirely clear if the backend is allowed to send them in a different order.
 *
 * Also note that this machine is used for both executing a portal as well as resuming a previously executed one.
 */
class ExecuteMachine(req: Request.Execute, parameters: ConnectionParameters) extends StateMachine[Response.QueryResponse] {

  sealed trait State
  case object Binding extends State
  case object Describing extends State
  case object ExecutingCommand extends State
  case class Executing(r: RowDescription) extends State
  case class StreamResult(rowDescription: RowDescription, pipe: Pipe[DataRow], lastWrite: Future[Unit]) extends State {
    def append(row: DataRow): StreamResult =
      StreamResult(rowDescription, pipe, lastWrite before pipe.write(row))
    def resultSet: ResultSet = ResultSet(rowDescription.rowFields, pipe.map(_.values), parameters)
  }
  case class Syncing(response: Option[Try[Response.QueryResponse]]) extends State

  override def start: TransitionResult[State, Response.QueryResponse] = req match {
    case Request.ExecutePortal(prepared, parameters, portalName, maxResults) =>
      Transition(
        Binding,
        SendSeveral(
          Bind(
            portal = portalName,
            statement = prepared.name,
            formats = Nil, // TODO: deal with parameters
            values = parameters, // TODO: deal with parameters
            resultFormats = Format.Binary :: Nil // request all results in binary format
          ),
          Describe(portalName, DescriptionTarget.Portal), // TODO: we can avoid sending this one when the Prepare phase already returned NoData.
          Execute(portalName, maxResults),
          Flush
        )
      )
    case Request.ResumePortal(portalName, maxResults) =>
      Transition(
        Describing,
        SendSeveral(
          // TODO: we can avoid this one by reusing the one in the first execution. Not sure what the best API for this is yet.
          Describe(portalName, DescriptionTarget.Portal),
          Execute(portalName, maxResults),
          Flush
        )
      )

  }

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
    case (r: StreamResult, PortalSuspended) =>
      r.lastWrite.liftToTry.unit before r.pipe.close()
      // TODO: the ReadyForQuery here is fake
      Complete(ReadyForQuery(InTx), None)
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

    case (state, msg) => throw PgSqlNoSuchTransition("ExecuteMachine", state, msg)
  }

}
