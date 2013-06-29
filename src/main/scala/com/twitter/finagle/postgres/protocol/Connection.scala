package com.twitter.finagle.postgres.protocol

import com.twitter.logging.Logger
import java.util.concurrent.ConcurrentLinkedQueue
import collection.mutable.ListBuffer


trait State

case object AuthenticationRequired extends State

case object AuthenticationInProgress extends State

case object AwaitingPassword extends State

case class AggregatingAuthData(statuses: Map[String, String], processId: Int, secretKey: Int) extends State

case object Connected extends State

case object Parsing extends State

case object Binding extends State

case object SimpleQuery extends State

case class AggregateRows(fields: IndexedSeq[Field], buff: ListBuffer[DataRow] = ListBuffer()) extends State

case class EmitOnReadyForQuery[R <: PgResponse](emit: R) extends State

class ConnectionStateMachine(state: State = AuthenticationRequired) extends StateMachine[Message, PgResponse, State] {
  private[this] val logger = Logger("pgsql state machine")

  startState(state)

  transition {
    case (StartupMessage(_, _), AuthenticationRequired) => (None, AuthenticationInProgress)
    case (AuthenticationOk(), AuthenticationInProgress) => (None, AggregatingAuthData(Map(), -1, -1))
    case (AuthenticationCleartextPassword(), AuthenticationInProgress) => (Some(PasswordRequired(ClearText)), AwaitingPassword)
    case (AuthenticationMD5Password(salt), AuthenticationInProgress) => (Some(PasswordRequired(Md5(salt))), AwaitingPassword)
    case (PasswordMessage(_), AwaitingPassword) => (None, AuthenticationInProgress)

    case (ErrorResponse(details), AuthenticationRequired | AuthenticationInProgress | AwaitingPassword) => (Some(Error(details)), AuthenticationRequired)
  }

  transition {
    case (ParameterStatus(name, value), AggregatingAuthData(statuses, processId, secretKey)) => (None, AggregatingAuthData(statuses = statuses + (name -> value), processId, secretKey))
    case (BackendKeyData(processId, secretKey), AggregatingAuthData(statuses, _, _)) => (None, AggregatingAuthData(statuses, processId, secretKey))
    case (ReadyForQuery(_), AggregatingAuthData(statuses, processId, secretKey)) => (Some(AuthenticatedResponse(statuses, processId, secretKey)), Connected)

    case (ErrorResponse(details), AggregatingAuthData(_, _, _)) => (Some(Error(details)), AuthenticationRequired)
  }

  transition {
    case (Query(_), Connected) => (None, SimpleQuery)
    case (Parse(_, _, _), Connected) => (None, Parsing)
    case (Bind(_, _, _, _, _), Connected) => (None, Binding)
    case (ErrorResponse(details), Connected) => (Some(Error(details)), Connected)
  }

  transition {
    case (EmptyQueryResponse, SimpleQuery) => (None, EmitOnReadyForQuery(SelectResult(IndexedSeq(), List())))
    case (CommandComplete(CreateTable), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(DropTable), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(1)))
    case (CommandComplete(Insert(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (CommandComplete(Update(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (CommandComplete(Delete(count)), SimpleQuery) => (None, EmitOnReadyForQuery(CommandCompleteResponse(count)))
    case (RowDescription(fields), SimpleQuery) => (None, AggregateRows(fields.map(f => Field(f.name, f.fieldFormat, f.dataType))))
    case (ErrorResponse(details), SimpleQuery) => (None, EmitOnReadyForQuery(Error(details)))
  }

  transition {
    case (row: DataRow, state: AggregateRows) =>
      state.buff += row
      (None, state)
    case (CommandComplete(Select(count)), AggregateRows(fields, rows)) => (None, EmitOnReadyForQuery(SelectResult(fields, rows.toList)))
    case (ErrorResponse(details), AggregateRows(_, _)) => (Some(Error(details)), Connected)
  }

  transition {
    case (ReadyForQuery(_), EmitOnReadyForQuery(response)) => (Some(response), Connected)
    case (ErrorResponse(details), EmitOnReadyForQuery(response)) => (Some(Error(details)), Connected)
  }

  transition {
    case (NoticeResponse(details), s) =>
      logger.ifInfo("Notice from server " + details)
      (None, s)
    case (notification: NotificationResponse, s) =>
      logger.ifInfo("Notification from server " + notification)
      (None, s)
    case (ParameterStatus(name, value), s) =>
      logger.ifInfo("Params changed " + name + " " + value)
      (None, s)

  }

}


class Connection {
  private[this] val logger = Logger("connection")

  private[this] val frontendQueue = new ConcurrentLinkedQueue[FrontendMessage]

  private[this] val stateMachine = new ConnectionStateMachine()
  @volatile private[this] var busy = false

  def send(msg: FrontendMessage) {
    logger.ifDebug("Frontend message accepted " + msg)
    frontendQueue.offer(msg)
  }

  def receive(msg: BackendMessage): Option[PgResponse] = {
    logger.ifDebug("Backend message received " + msg)

    if (!busy) {
      val pending = frontendQueue.peek
      if (pending == null) {
        logger.ifWarning("Backend message respondend and no frontend messages were sent")
      } else {
        logger.ifDebug("Frontend message is pending " + pending)
        val _ = stateMachine.onEvent(pending)
        busy = true
      }
    }

    val result = stateMachine.onEvent(msg)
    if (result.isDefined) {
      busy = false
      frontendQueue.poll
    }
    logger.ifDebug("Emiting result " + result)
    result
  }

}
