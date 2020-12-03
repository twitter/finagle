package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Buf

sealed trait BackendMessage
object BackendMessage {

  sealed trait CommandTag {
    def rows: Int
  }
  object CommandTag {
    case class Insert(rows: Int) extends CommandTag
    case class Delete(rows: Int) extends CommandTag
    case class Update(rows: Int) extends CommandTag
    case class Select(rows: Int) extends CommandTag
    case class Move(rows: Int) extends CommandTag
    case class Fetch(rows: Int) extends CommandTag
    case class Other(value: String) extends CommandTag {
      def rows: Int = throw PgSqlUnsupportedError(s"Unsupported command tag: $value")
    }
  }
  case class CommandComplete(commandTag: CommandTag) extends BackendMessage
  case object EmptyQueryResponse extends BackendMessage

  case class RowDescription(rowFields: IndexedSeq[FieldDescription]) extends BackendMessage
  case class DataRow(values: IndexedSeq[WireValue]) extends BackendMessage

  sealed trait AuthenticationMessage extends BackendMessage
  case object AuthenticationOk extends AuthenticationMessage
  case object AuthenticationKerberosV5 extends AuthenticationMessage
  case object AuthenticationCleartextPassword extends AuthenticationMessage
  case object AuthenticationSCMCredential extends AuthenticationMessage
  case class AuthenticationMD5Password(salt: Buf) extends AuthenticationMessage
  case object AuthenticationGSS extends AuthenticationMessage
  case object AuthenticationSSPI extends AuthenticationMessage
  case class AuthenticationGSSContinue(authData: Buf) extends AuthenticationMessage
  case class AuthenticationSASL(mechanism: String) extends AuthenticationMessage
  case class AuthenticationSASLContinue(challenge: Buf) extends AuthenticationMessage
  case class AuthenticationSASLFinal(challenge: Buf) extends AuthenticationMessage

  sealed trait Parameter
  object Parameter {
    case object ServerVersion extends Parameter
    case object ServerEncoding extends Parameter
    case object ClientEncoding extends Parameter
    case object ApplicationName extends Parameter
    case object IsSuperUser extends Parameter
    case object SessionAuthorization extends Parameter
    case object DateStyle extends Parameter
    case object IntervalStyle extends Parameter
    case object TimeZone extends Parameter
    case object IntegerDateTimes extends Parameter
    case object StandardConformingStrings extends Parameter
    case class Other(name: String) extends Parameter
  }
  case class ParameterStatus(key: Parameter, value: String) extends BackendMessage

  case class BackendKeyData(pid: Int, secret: Int) extends BackendMessage

  sealed trait TxState
  case object NoTx extends TxState
  case object InTx extends TxState
  case object FailedTx extends TxState
  case class ReadyForQuery(state: TxState) extends BackendMessage

  // https://www.postgresql.org/docs/current/protocol-error-fields.html
  sealed trait Field
  object Field {
    case object LocalizedSeverity extends Field
    case object Severity extends Field
    case object Code extends Field
    case object Message extends Field
    case object Detail extends Field
    case object Hint extends Field
    case object Position extends Field
    case object InternalPosition extends Field
    case object InternalQuery extends Field
    case object Where extends Field
    case object Schema extends Field
    case object Table extends Field
    case object Column extends Field
    case object DataType extends Field
    case object Constraint extends Field
    case object File extends Field
    case object Line extends Field
    case object Routine extends Field

    case class Unknown(value: Char) extends Field
  }

  // TODO: parse out the fields to expose the category and sql state.
  case class NoticeResponse(values: Map[Field, String]) extends BackendMessage
  case class ErrorResponse(values: Map[Field, String]) extends BackendMessage

  // extended query
  case object ParseComplete extends BackendMessage
  case object BindComplete extends BackendMessage
  case class ParameterDescription(parameters: IndexedSeq[Oid]) extends BackendMessage
  case object NoData extends BackendMessage
  case object PortalSuspended extends BackendMessage
  case object CloseComplete extends BackendMessage

  // COPY
  case class CopyData(bytes: Buf) extends BackendMessage
  case object CopyDone extends BackendMessage
  case class CopyFail(msg: String) extends BackendMessage
  case class CopyInResponse(
    overallFormat: Format,
    columnsFormat: IndexedSeq[Format]
  ) extends BackendMessage

  case class CopyOutResponse(
    overallFormat: Format,
    columnsFormat: IndexedSeq[Format]
  ) extends BackendMessage
}
