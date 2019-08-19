package com.twitter.finagle.postgres.connection

import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.postgres.codec.Errors
import com.twitter.finagle.postgres.messages._
import com.twitter.logging.Logger
import com.twitter.util.{Promise, Return, Throw}

/*
 * State machine that captures transitions between states.
 *
 * See associated Postgres documentation: http://www.postgresql.org/docs/9.0/static/protocol-flow.html
 */
class ConnectionStateMachine(state: State = AuthenticationRequired, val id: Int) extends StateMachine[Message, PgResponse, State] {
  private[this] val logger = Logger(s"${getClass.getName}.psql state machine:$id")

  startState(state)

  transition {
    case (SslRequestMessage(), RequestingSsl) => (None, AwaitingSslResponse)
    case (SwitchToSsl, AwaitingSslResponse) => (Some(SslSupportedResponse), AuthenticationRequired)
    case (SslNotSupported, AwaitingSslResponse) => (Some(SslNotSupportedResponse), AuthenticationRequired)
  }

  transition {
    case (StartupMessage(_, _), AuthenticationRequired) => (None, AuthenticationInProgress)
    case (AuthenticationOk(), AuthenticationInProgress) => (None, AggregatingAuthData(Map(), -1, -1))
    case (AuthenticationCleartextPassword(), AuthenticationInProgress) =>
      (Some(PasswordRequired(ClearText)), AwaitingPassword)
    case (AuthenticationMD5Password(salt), AuthenticationInProgress) =>
      (Some(PasswordRequired(Md5(salt))), AwaitingPassword)
    case (PasswordMessage(_), AwaitingPassword) => (None, AuthenticationInProgress)

    case (ErrorResponse(details), AuthenticationRequired | AuthenticationInProgress | AwaitingPassword) =>
      (Some(Error(details)), AuthenticationRequired)
  }

  transition {
    case (ParameterStatus(name, value), AggregatingAuthData(statuses, processId, secretKey)) =>
      (None, AggregatingAuthData(statuses = statuses + (name -> value), processId, secretKey))
    case (BackendKeyData(processId, secretKey), AggregatingAuthData(statuses, _, _)) =>
      (None, AggregatingAuthData(statuses, processId, secretKey))
    case (ReadyForQuery(_), AggregatingAuthData(statuses, processId, secretKey)) =>
      (Some(AuthenticatedResponse(statuses, processId, secretKey)), Connected)

    case (ErrorResponse(details), AggregatingAuthData(_, _, _)) => (Some(Error(details)), AuthenticationRequired)
  }

  transition {
    case (Query(_), Connected) => (None, SimpleQuery)
    case (Parse(_, _, _), Connected) => (None, Parsing)
    case (Bind(_, _, _, _, _), Connected) => (None, Binding)
    case (Describe(_, _), Connected) => (None, AwaitParamsDescription)
    case (Execute(_, _), Connected) => (None, ExecutePreparedStatement)
    case (Sync, Connected) => (None, Syncing)
    case (Sync, state: ExtendedQueryState) => (None, Syncing)
    case (ErrorResponse(details), Connected) => (Some(Error(details)), Connected)
  }

  transition {
    case (ParameterDescription(types), AwaitParamsDescription) => (None, AwaitRowDescription(types))
    case (RowDescription(fields), AwaitParamsDescription) =>
      (Some(RowDescriptions(fields.map(f => Field(f.name, f.fieldFormat, f.dataType)))), Connected)
    case (NoData, AwaitParamsDescription) => (Some(RowDescriptions(Array.empty)), Connected)
    case (ErrorResponse(details), AwaitParamsDescription) => (Some(Error(details)), Connected)
  }

  transition {
    case (RowDescription(fields), AwaitRowDescription(types)) =>
      (Some(RowDescriptions(fields.map(f => Field(f.name, f.fieldFormat, f.dataType)))), Connected)
    case (NoData, AwaitRowDescription(types)) => (Some(RowDescriptions(Array.empty)), Connected)
    case (ErrorResponse(details), AwaitRowDescription(_)) => (Some(Error(details)), Connected)
  }

  transition {
    case (BindComplete, Binding) => (Some(BindCompletedResponse), Connected)
    case (ErrorResponse(details), Binding) => (Some(Error(details)), Connected)
  }

  transition {
    case (ParseComplete, Parsing) => (Some(ParseCompletedResponse), Connected)
    case (ErrorResponse(details), Parsing) => (Some(Error(details)), Connected)
  }

  transition {
    case (ReadyForQuery(_), Syncing) => (Some(ReadyForQueryResponse), Connected)
    case (ErrorResponse(details), Syncing) => throw new IllegalStateException("Received an error in response to Sync")
  }

  transition {
    case (EmptyQueryResponse, SimpleQuery) =>
      (None, EmitOnReadyForQuery(SelectResult.Empty))
    case (CommandComplete(CreateExtension | CreateFunction | CreateIndex | CreateTable | CreateTrigger | CreateType), SimpleQuery) =>
      (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(DropTable), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Insert(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (CommandComplete(Update(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (CommandComplete(Delete(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (CommandComplete(DiscardAll), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Begin | Savepoint | Release | RollBack | Commit), SimpleQuery) =>
      (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Do), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))

    case (RowDescription(fields), SimpleQuery) =>
      val complete = new Promise[Unit]()
      val nextRow = StreamRows(complete, extended = false)
      (Some(SelectResult(fields.map(f => Field(f.name, f.fieldFormat, f.dataType)), nextRow.asyncStream)(complete)), nextRow)
    case (ErrorResponse(details), SimpleQuery) =>
      (None, EmitOnReadyForQuery(Error(details)))
  }

  transition {
    //for multiple commands in one query, we recieve multiple CommandComplete tags before becoming ready
    case (CommandComplete(_), EmitOnReadyForQuery(old)) => (None, EmitOnReadyForQuery(old))
  }

  transition {
    case (EmptyQueryResponse, ExecutePreparedStatement) => (Some(SelectResult.Empty), Connected)
    case (CommandComplete(CreateExtension | CreateFunction | CreateIndex | CreateTable | CreateType | CreateTrigger), ExecutePreparedStatement) =>
      (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(DropTable), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(Insert(count)), ExecutePreparedStatement) => (Some(CommandCompleteResponse(count)), Connected)
    case (CommandComplete(Update(count)), ExecutePreparedStatement) => (Some(CommandCompleteResponse(count)), Connected)
    case (CommandComplete(Delete(count)), ExecutePreparedStatement) => (Some(CommandCompleteResponse(count)), Connected)
    case (CommandComplete(Begin), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(Savepoint), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(RollBack), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(Commit), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(Do), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (row: DataRow, ExecutePreparedStatement) =>
      val complete = new Promise[Unit]
      val nextRow = StreamRows(complete, extended = true)
      val thisRow = AsyncStream.mk(row, nextRow.asyncStream)
      val response = Rows(thisRow)(complete)
      (Some(response), nextRow)
    case (CommandComplete(Select(0)), ExecutePreparedStatement) =>
      (Some(Rows.Empty), Connected)
    case (ErrorResponse(details), ExecutePreparedStatement) =>
      (Some(Error(details)), Connected)
  }

  fullTransition {
    case (row: DataRow, StreamRows(complete, extended, thisRow)) =>
      val nextRow = StreamRows(complete, extended)
      thisRow.setValue(AsyncStream.mk(row, nextRow.asyncStream))
      (None, nextRow)
    case (PortalSuspended, StreamRows(complete, _, thisRow)) =>
      thisRow.setValue(AsyncStream.empty)
      (Some(StateMachine.Complete(complete, Return.Unit)), Connected)
    case (CommandComplete(_), StreamRows(complete, extended, thisRow)) =>
      thisRow.setValue(AsyncStream.empty)
      val response = StateMachine.Complete(complete, Return.Unit)
      if (extended) {
        // in extended mode, we don't expect a ReadyForQuery, so we respond now
        (Some(response), Connected)
      } else {
        (None, EmitOnReadyForQuery(response))
      }
    case (ErrorResponse(details), StreamRows(complete, extended, thisRow)) =>
      val exn = Errors.server(Error(details), None)
      thisRow.setValue(AsyncStream.exception(exn))
      val response = StateMachine.Complete(complete, Throw(exn))
      if (extended) {
        // in extended mode, we don't expect a ReadyForQuery, so we respond now
        (Some(response), Connected)
      } else {
        (None, EmitOnReadyForQuery(response))
      }
  }

  fullTransition {
    case (ReadyForQuery(_), EmitOnReadyForQuery(response)) => (Some(response), Connected)
    case (ErrorResponse(details), EmitOnReadyForQuery(StateMachine.Response(_))) =>
      (Some(StateMachine.Response(Error(details))), Connected)
    case (ErrorResponse(details), EmitOnReadyForQuery(StateMachine.Complete(promise, _))) =>
      val exn = Errors.server(Error(details), None)
      (Some(StateMachine.Complete(promise, Throw(exn))), Connected)
  }

  transition {
    case (Terminate, _) => (Some(com.twitter.finagle.postgres.messages.Terminated), Terminated)
    case (_, Terminated) => (Some(com.twitter.finagle.postgres.messages.Terminated), Terminated)
  }

  transition {
    case (NoticeResponse(details), s) =>
      logger.ifDebug("Notice from server: %s".format(details))
      (None, s)
    case (notification: NotificationResponse, s) =>
      logger.ifDebug("Notification from server: %s".format(notification))
      (None, s)
    case (ParameterStatus(name, value), s) =>
      logger.ifDebug("Params changed: %s %s".format(name, value))
      (None, s)
  }
}
