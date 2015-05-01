package com.twitter.finagle.postgres.messages

import org.jboss.netty.buffer.ChannelBuffer

/**
 * Responses sent from Postgres back to the client.
 */
trait BackendMessage extends Message

case object SwitchToSsl extends BackendMessage

case object SslNotSupported extends BackendMessage

case class ErrorResponse(msg: Option[String] = None) extends BackendMessage

case class NoticeResponse(msg: Option[String]) extends BackendMessage

case class NotificationResponse(processId: Int, channel: String, payload: String) extends BackendMessage

case class AuthenticationOk() extends BackendMessage

case class AuthenticationMD5Password(salt: Array[Byte]) extends BackendMessage

case class AuthenticationCleartextPassword() extends BackendMessage

case class ParameterStatus(name: String, value: String) extends BackendMessage

case class BackendKeyData(processId: Int, secretKey: Int) extends BackendMessage

case class ParameterDescription(types: IndexedSeq[Int]) extends BackendMessage

case class RowDescription(fields: IndexedSeq[FieldDescription]) extends BackendMessage

case class FieldDescription(
    name: String,
    tableId: Int,
    columnNumber: Int,
    dataType: Int,
    dataTypeSize: Int,
    dataTypeMondifier: Int,
    fieldFormat: Int)

case class DataRow(data: IndexedSeq[ChannelBuffer]) extends BackendMessage

/*
 * Sub-message types used to complete a command.
 */
sealed trait CommandCompleteStatus

case object CreateTable extends CommandCompleteStatus

case object DropTable extends CommandCompleteStatus

case object DiscardAll extends CommandCompleteStatus

case class Insert(count : Int) extends CommandCompleteStatus

case class Update(count : Int) extends CommandCompleteStatus

case class Delete(count : Int) extends CommandCompleteStatus

case class Select(count: Int) extends CommandCompleteStatus

case object Begin extends CommandCompleteStatus

case object Savepoint extends CommandCompleteStatus

case object RollBack extends CommandCompleteStatus

case object Commit extends CommandCompleteStatus

case class CommandComplete(status: CommandCompleteStatus) extends BackendMessage

case class ReadyForQuery(status: Char) extends BackendMessage

case object ParseComplete extends BackendMessage

case object BindComplete extends BackendMessage

case object NoData extends BackendMessage

case object PortalSuspended extends BackendMessage

case object EmptyQueryResponse extends BackendMessage
