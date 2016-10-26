package com.twitter.finagle.postgres.connection

import com.twitter.finagle.postgres.messages._
import com.twitter.logging.Logger

import scala.collection.mutable.ListBuffer

/*
 * State machine that captures transitions between states.
 *
 * See associated Postgres documentation: http://www.postgresql.org/docs/9.0/static/protocol-flow.html
 */
class ConnectionStateMachine(state: State = AuthenticationRequired) extends StateMachine[Message, PgResponse, State] {
  private[this] val logger = Logger("psql state machine")

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
    case (NoData, AwaitParamsDescription) => (Some(RowDescriptions(IndexedSeq())), Connected)
    case (ErrorResponse(details), AwaitParamsDescription) => (Some(Error(details)), Connected)
  }

  transition {
    case (RowDescription(fields), AwaitRowDescription(types)) =>
      (Some(RowDescriptions(fields.map(f => Field(f.name, f.fieldFormat, f.dataType)))), Connected)
    case (NoData, AwaitRowDescription(types)) => (Some(RowDescriptions(IndexedSeq())), Connected)
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
    case (ErrorResponse(details), Syncing) => (Some(Error(details)), Connected)
  }

  transition {
    case (EmptyQueryResponse, SimpleQuery) => (None, EmitOnReadyForQuery(SelectResult(IndexedSeq(), List())))
    case (CommandComplete(CreateTable), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(CreateType), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(CreateExtension), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(DropTable), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Insert(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (CommandComplete(Update(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (CommandComplete(Delete(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (CommandComplete(DiscardAll), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Begin), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Savepoint), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(RollBack), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Commit), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Do), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))

    case (RowDescription(fields), SimpleQuery) =>
      (None, AggregateRows(fields.map(f => Field(f.name, f.fieldFormat, f.dataType))))
    case (ErrorResponse(details), SimpleQuery) => (None, EmitOnReadyForQuery(Error(details)))
  }

  transition {
    //for multiple commands in one query, we recieve multiple CommandComplete tags before becoming ready
    case (CommandComplete(_), EmitOnReadyForQuery(old)) => (None, EmitOnReadyForQuery(old))
  }

  transition {
    case (EmptyQueryResponse, ExecutePreparedStatement) => (Some(SelectResult(IndexedSeq(), List())), Connected)
    case (CommandComplete(CreateTable), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(CreateType), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(CreateExtension), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(DropTable), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(Insert(count)), ExecutePreparedStatement) => (Some(CommandCompleteResponse(count)), Connected)
    case (CommandComplete(Update(count)), ExecutePreparedStatement) => (Some(CommandCompleteResponse(count)), Connected)
    case (CommandComplete(Delete(count)), ExecutePreparedStatement) => (Some(CommandCompleteResponse(count)), Connected)
    case (CommandComplete(Begin), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(Savepoint), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(RollBack), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(Commit), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (CommandComplete(Do), ExecutePreparedStatement) => (Some(CommandCompleteResponse(1)), Connected)
    case (row:DataRow, ExecutePreparedStatement) => (None, AggregateRowsWithoutFields(ListBuffer(row)))
    case (row:DataRow, state:AggregateRowsWithoutFields) =>
      state.buff += row
      (None, state)
    case (PortalSuspended, AggregateRowsWithoutFields(buff)) => (Some(Rows(buff.toList, completed = false)), Connected)
    case (CommandComplete(Select(0)), ExecutePreparedStatement) => (Some(Rows(List.empty, completed = true)), Connected)
    case (CommandComplete(Select(_)), AggregateRowsWithoutFields(buff)) =>
      (Some(Rows(buff.toList, completed = true)), Connected)
    case (CommandComplete(Insert(_)), AggregateRowsWithoutFields(buff)) =>
      (Some(Rows(buff.toList, completed = true)), Connected)
    case (CommandComplete(Update(_)), AggregateRowsWithoutFields(buff)) =>
      (Some(Rows(buff.toList, completed = true)), Connected)
    case (CommandComplete(Delete(_)), AggregateRowsWithoutFields(buff)) => 
      (Some(Rows(buff.toList, completed = true)), Connected)
    case (ErrorResponse(details), ExecutePreparedStatement) => (Some(Error(details)), Connected)
    case (ErrorResponse(details), AggregateRowsWithoutFields(_)) => (Some(Error(details)), Connected)
  }

  transition {
    case (row: DataRow, state: AggregateRows) =>
      state.buff += row
      (None, state)
    case (CommandComplete(Select(count)), AggregateRows(fields, rows)) =>
      (None, EmitOnReadyForQuery(SelectResult(fields, rows.toList)))
    case (ErrorResponse(details), AggregateRows(_, _)) => (Some(Error(details)), Connected)
  }

  transition {
    case (ReadyForQuery(_), EmitOnReadyForQuery(response)) => (Some(response), Connected)
    case (ErrorResponse(details), EmitOnReadyForQuery(response)) => (Some(Error(details)), Connected)
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
