package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.BackendMessage
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
import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.FailedTx
import com.twitter.finagle.postgresql.BackendMessage.Field
import com.twitter.finagle.postgresql.BackendMessage.FieldDescription
import com.twitter.finagle.postgresql.BackendMessage.InTx
import com.twitter.finagle.postgresql.BackendMessage.NoTx
import com.twitter.finagle.postgresql.BackendMessage.NoticeResponse
import com.twitter.finagle.postgresql.BackendMessage.ParameterDescription
import com.twitter.finagle.postgresql.BackendMessage.ParameterStatus
import com.twitter.finagle.postgresql.BackendMessage.ParseComplete
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.Types.AttributeId
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

trait MessageDecoder[M <: BackendMessage] {
  def decode(b: PgBuf.Reader): Try[M]
}

object MessageDecoder {

  def decode[M <: BackendMessage](reader: PgBuf.Reader)(implicit decoder: MessageDecoder[M]) =
    decoder.decode(reader)

  def fromPacket(p: Packet): Try[BackendMessage] = {
    lazy val reader = PgBuf.reader(p.body)
    p.cmd.getOrElse(Throw(new IllegalStateException("invalid backend packet, missing type."))) match {
      case '1' => Return(ParseComplete)
      case '2' => Return(BindComplete)
      case 'C' => decode[CommandComplete](reader)
      case 'D' => decode[DataRow](reader)
      case 'E' => decode[ErrorResponse](reader)
      case 'I' => Return(EmptyQueryResponse)
      case 'K' => decode[BackendKeyData](reader)
      case 'N' => decode[NoticeResponse](reader)
      case 'R' => decode[AuthenticationMessage](reader)
      case 'S' => decode[ParameterStatus](reader)
      case 't' => decode[ParameterDescription](reader)
      case 'T' => decode[RowDescription](reader)
      case 'Z' => decode[ReadyForQuery](reader)
      case byte => sys.error(s"unimplemented message $byte")
    }
  }

  def apply[M <: BackendMessage](f: PgBuf.Reader => M): MessageDecoder[M] = reader => Try(f(reader))

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

    def loop(fields: Map[Field, String]): Map[Field, String] =
      nextField match {
        case None => fields
        case Some(field) =>
          val value = reader.string()
          loop(fields + (field -> value))
      }

    loop(Map.empty)
  }

  implicit lazy val errorResponseDecoder: MessageDecoder[ErrorResponse] = MessageDecoder { reader =>
    ErrorResponse(readFields(reader))
  }

  implicit lazy val noticeResponseDecoder: MessageDecoder[NoticeResponse] = MessageDecoder { reader =>
    NoticeResponse(readFields(reader))
  }

  implicit lazy val backendKeyDataDecoder: MessageDecoder[BackendKeyData] = MessageDecoder { reader =>
    BackendKeyData(reader.int(), reader.int())
  }

  implicit lazy val commandCompleteDecoder: MessageDecoder[CommandComplete] = MessageDecoder { reader =>
    CommandComplete(reader.string())
  }

  implicit lazy val authenticationMessageDecoder: MessageDecoder[AuthenticationMessage] = MessageDecoder { reader =>
    reader.int() match {
      case 0 => AuthenticationOk
      case 2 => AuthenticationKerberosV5
      case 3 => AuthenticationCleartextPassword
      case 5 => AuthenticationMD5Password(reader.buf(4))
      case 6 => AuthenticationSCMCredential
      case 7 => AuthenticationGSS
      case 8 => AuthenticationGSSContinue(reader.remainingBuf())
      case 9 => AuthenticationSSPI
      case 10 => AuthenticationSASL(reader.string())
      case 11 => AuthenticationSASLContinue(reader.remainingBuf())
      case 12 => AuthenticationSASLFinal(reader.remainingBuf())
    }
  }

  implicit lazy val parameterStatusDecoder: MessageDecoder[ParameterStatus] = MessageDecoder { reader =>
    ParameterStatus(reader.string(), reader.string())
  }

  implicit lazy val readyForQueryDecoder: MessageDecoder[ReadyForQuery] = MessageDecoder { reader =>
    val state = reader.byte().toChar match {
      case 'I' => NoTx
      case 'T' => InTx
      case 'F' => FailedTx
    }
    ReadyForQuery(state)
  }

  implicit lazy val rowDescriptionDecoder: MessageDecoder[RowDescription] = MessageDecoder { reader =>
    RowDescription(
      reader.collect { r =>
        FieldDescription(
          name = r.string(),
          tableOid = r.int() match {
            case 0 => None
            case oid => Some(Oid(oid))
          },
          tableAttributeId = r.short() match {
            case 0 => None
            case attrId => Some(AttributeId(attrId))
          },
          dataType = Oid(r.int()),
          dataTypeSize = r.short(),
          typeModifier = r.int(),
          format = r.format()
        )
      }
    )
  }

  implicit lazy val dataRowDecoder: MessageDecoder[DataRow] = MessageDecoder { reader =>
    DataRow(
      reader.collect { _.value() }
    )
  }

  implicit lazy val parameterDescriptionDecoder: MessageDecoder[ParameterDescription] = MessageDecoder { reader =>
    ParameterDescription(
      reader.collect { r => Oid(r.int()) }
    )
  }
}
