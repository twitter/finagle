package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationCleartextPassword
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationGSS
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationGSSContinue
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationKerberosV5
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationMD5Password
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationMessage
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationOk
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSASL
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSASLContinue
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSASLFinal
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSCMCredential
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSSPI
import com.twitter.finagle.postgresql.BackendMessage.BackendKeyData
import com.twitter.finagle.postgresql.BackendMessage.BindComplete
import com.twitter.finagle.postgresql.BackendMessage.CloseComplete
import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.CommandTag
import com.twitter.finagle.postgresql.BackendMessage.CopyData
import com.twitter.finagle.postgresql.BackendMessage.CopyDone
import com.twitter.finagle.postgresql.BackendMessage.CopyInResponse
import com.twitter.finagle.postgresql.BackendMessage.CopyOutResponse
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.FailedTx
import com.twitter.finagle.postgresql.BackendMessage.Field
import com.twitter.finagle.postgresql.BackendMessage.InTx
import com.twitter.finagle.postgresql.BackendMessage.NoData
import com.twitter.finagle.postgresql.BackendMessage.NoTx
import com.twitter.finagle.postgresql.BackendMessage.NoticeResponse
import com.twitter.finagle.postgresql.BackendMessage.Parameter
import com.twitter.finagle.postgresql.BackendMessage.ParameterDescription
import com.twitter.finagle.postgresql.BackendMessage.ParameterStatus
import com.twitter.finagle.postgresql.BackendMessage.ParseComplete
import com.twitter.finagle.postgresql.BackendMessage.PortalSuspended
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.Types.AttributeId
import com.twitter.finagle.postgresql.Types.FieldDescription
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.io.Buf
import scala.annotation.tailrec

/**
 * A typeclass for decoding [[BackendMessage]] from a [[Packet]].
 *
 * @see [[MessageEncoder]]
 * @see [[PgBuf.Reader]]
 */
trait MessageDecoder[M <: BackendMessage] {
  def decode(b: PgBuf.Reader): M
}

object MessageDecoder {

  def decode[M <: BackendMessage](reader: PgBuf.Reader)(implicit decoder: MessageDecoder[M]): M =
    decoder.decode(reader)

  def fromBuf(buf: Buf): BackendMessage = {
    val reader = PgBuf.reader(buf)
    val cmd = reader.byte()

    if (reader.remaining >= 4) {
      // skip the 4 byte packet length
      reader.skip(4)
    }

    val ret = cmd match {
      case '1' => ParseComplete
      case '2' => BindComplete
      case '3' => CloseComplete
      case 'c' => CopyDone
      case 'C' => decode[CommandComplete](reader)
      case 'd' => decode[CopyData](reader)
      case 'D' => decode[DataRow](reader)
      case 'E' => decode[ErrorResponse](reader)
      case 'G' => decode[CopyInResponse](reader)
      case 'H' => decode[CopyOutResponse](reader)
      case 'I' => EmptyQueryResponse
      case 'K' => decode[BackendKeyData](reader)
      case 'n' => NoData
      case 'N' => decode[NoticeResponse](reader)
      case 'R' => decode[AuthenticationMessage](reader)
      case 's' => PortalSuspended
      case 'S' => decode[ParameterStatus](reader)
      case 't' => decode[ParameterDescription](reader)
      case 'T' => decode[RowDescription](reader)
      case 'Z' => decode[ReadyForQuery](reader)
      case byte => throw new PgSqlClientError(s"unimplemented message '${byte.toChar}'")
    }
    if (reader.remaining != 0) {
      throw new PgSqlClientError("message decoding did not consume the entire packet")
    }
    ret
  }

  def apply[M <: BackendMessage](f: PgBuf.Reader => M): MessageDecoder[M] = reader => f(reader)

  def readFields(reader: PgBuf.Reader): Map[Field, String] = {
    import Field._
    def nextField: Option[Field] =
      reader.byte().toChar match {
        case 0 => None
        case 'S' => Some(LocalizedSeverity)
        case 'V' => Some(Severity)
        case 'C' => Some(Code)
        case 'M' => Some(Message)
        case 'D' => Some(Detail)
        case 'H' => Some(Hint)
        case 'P' => Some(Position)
        case 'p' => Some(InternalPosition)
        case 'q' => Some(InternalQuery)
        case 'W' => Some(Where)
        case 's' => Some(Schema)
        case 't' => Some(Table)
        case 'c' => Some(Column)
        case 'd' => Some(DataType)
        case 'n' => Some(Constraint)
        case 'F' => Some(File)
        case 'L' => Some(Line)
        case 'R' => Some(Routine)
        case unk => Some(Unknown(unk))
      }

    @tailrec
    def loop(fields: Map[Field, String]): Map[Field, String] =
      nextField match {
        case None => fields
        case Some(field) =>
          val value = reader.cstring()
          loop(fields + (field -> value))
      }

    loop(Map.empty)
  }

  implicit lazy val errorResponseDecoder: MessageDecoder[ErrorResponse] = MessageDecoder { reader =>
    ErrorResponse(readFields(reader))
  }

  implicit lazy val noticeResponseDecoder: MessageDecoder[NoticeResponse] = MessageDecoder {
    reader =>
      NoticeResponse(readFields(reader))
  }

  implicit lazy val backendKeyDataDecoder: MessageDecoder[BackendKeyData] = MessageDecoder {
    reader =>
      BackendKeyData(reader.int(), reader.int())
  }

  def commandTag(value: String): CommandTag =
    value.split(" ", 3).toList match {
      case "INSERT" :: _ :: rows :: Nil => CommandTag.AffectedRows(CommandTag.Insert, rows.toInt)
      case "DELETE" :: rows :: Nil => CommandTag.AffectedRows(CommandTag.Delete, rows.toInt)
      case "UPDATE" :: rows :: Nil => CommandTag.AffectedRows(CommandTag.Update, rows.toInt)
      case "SELECT" :: rows :: Nil => CommandTag.AffectedRows(CommandTag.Select, rows.toInt)
      case "MOVE" :: rows :: Nil => CommandTag.AffectedRows(CommandTag.Move, rows.toInt)
      case "FETCH" :: rows :: Nil => CommandTag.AffectedRows(CommandTag.Fetch, rows.toInt)
      case _ => CommandTag.Other(value)
    }

  implicit lazy val commandCompleteDecoder: MessageDecoder[CommandComplete] = MessageDecoder {
    reader =>
      CommandComplete(commandTag(reader.cstring()))
  }

  implicit lazy val authenticationMessageDecoder: MessageDecoder[AuthenticationMessage] =
    MessageDecoder { reader =>
      reader.int() match {
        case 0 => AuthenticationOk
        case 2 => AuthenticationKerberosV5
        case 3 => AuthenticationCleartextPassword
        case 5 => AuthenticationMD5Password(reader.buf(4))
        case 6 => AuthenticationSCMCredential
        case 7 => AuthenticationGSS
        case 8 => AuthenticationGSSContinue(reader.remainingBuf())
        case 9 => AuthenticationSSPI
        case 10 => AuthenticationSASL(reader.cstring())
        case 11 => AuthenticationSASLContinue(reader.remainingBuf())
        case 12 => AuthenticationSASLFinal(reader.remainingBuf())
      }
    }

  implicit lazy val parameterStatusDecoder: MessageDecoder[ParameterStatus] = MessageDecoder {
    reader =>
      val parameter = reader.cstring() match {
        case "server_version" => Parameter.ServerVersion
        case "server_encoding" => Parameter.ServerEncoding
        case "client_encoding" => Parameter.ClientEncoding
        case "application_name" => Parameter.ApplicationName
        case "is_superuser" => Parameter.IsSuperUser
        case "session_authorization" => Parameter.SessionAuthorization
        case "DateStyle" => Parameter.DateStyle
        case "IntervalStyle" => Parameter.IntervalStyle
        case "TimeZone" => Parameter.TimeZone
        case "integer_datetimes" => Parameter.IntegerDateTimes
        case "standard_conforming_strings" => Parameter.StandardConformingStrings
        case other => Parameter.Other(other)
      }
      ParameterStatus(parameter, reader.cstring())
  }

  implicit lazy val readyForQueryDecoder: MessageDecoder[ReadyForQuery] = MessageDecoder { reader =>
    val state = reader.byte().toChar match {
      case 'I' => NoTx
      case 'T' => InTx
      case 'F' => FailedTx
    }
    ReadyForQuery(state)
  }

  implicit lazy val rowDescriptionDecoder: MessageDecoder[RowDescription] = MessageDecoder {
    reader =>
      RowDescription(
        reader.collect { r =>
          FieldDescription(
            name = r.cstring(),
            tableOid = r.unsignedInt() match {
              case 0 => None
              case oid => Some(Oid(oid))
            },
            tableAttributeId = r.short() match {
              case 0 => None
              case attrId => Some(AttributeId(attrId))
            },
            dataType = Oid(r.unsignedInt()),
            dataTypeSize = r.short(),
            typeModifier = r.int(),
            format = r.format()
          )
        }
      )
  }

  implicit lazy val dataRowDecoder: MessageDecoder[DataRow] = MessageDecoder { reader =>
    DataRow(
      reader.collect(_.value())
    )
  }

  implicit lazy val parameterDescriptionDecoder: MessageDecoder[ParameterDescription] =
    MessageDecoder { reader =>
      ParameterDescription(
        reader.collect(r => Oid(r.unsignedInt()))
      )
    }

  implicit lazy val copyInResponseDecoder: MessageDecoder[CopyInResponse] = MessageDecoder {
    reader =>
      CopyInResponse(
        overallFormat = reader.byte() match {
          case 0 => Format.Text
          case 1 => Format.Binary
        },
        columnsFormat = reader.collect(_.format()),
      )
  }

  implicit lazy val copyOutResponseDecoder: MessageDecoder[CopyOutResponse] = MessageDecoder {
    reader =>
      CopyOutResponse(
        overallFormat = reader.byte() match {
          case 0 => Format.Text
          case 1 => Format.Binary
        },
        columnsFormat = reader.collect(_.format()),
      )
  }

  implicit lazy val copyDataDecoder: MessageDecoder[CopyData] = MessageDecoder { reader =>
    CopyData(reader.remainingBuf())
  }

}
